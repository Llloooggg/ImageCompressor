// Package media encodes one file into its canonical form using external tools,
// writing to a caller-provided temp path. It never touches the original and
// never decides about DB/rename — that is the runner's job (crash-safe protocol).
//
// Video encoding is GPU-first with CPU fallback: by default it tries a hardware
// AV1 encoder (nvenc/amf/qsv) and falls back to SVT-AV1 (CPU) on failure.
package media

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Llloooggg/ImageCompressor/internal/tools"
)

// ErrNotSmaller means no encoder produced a smaller file than the original.
var ErrNotSmaller = errors.New("no encoder reduced the size")

// Encoder modes.
const (
	EncAuto   = "auto"   // default: GPU if available, else CPU (SVT-AV1)
	EncGPU    = "gpu"    // GPU only (error if none)
	EncCPU    = "cpu"    // SVT-AV1 only
	EncNVENC  = "nvenc"  // force av1_nvenc
	EncAMF    = "amf"    // force av1_amf
	EncQSV    = "qsv"    // force av1_qsv
	EncLibaom = "libaom" // libaom-av1, smallest but slow (--max)
)

// Opts are the encoding knobs (already validated/normalized by config).
type Opts struct {
	PhotoQ       int     // 0..100
	PhotoLoss    bool    // pixel-exact AVIF
	PhotoSpeed   int     // avifenc --speed 0..10 (higher = faster, bigger; 6 = canon)
	PhotoVMAFMin float64 // min acceptable VMAF for ESCALATED photo levels (0 disables the guard)
	VideoCRF     int     // CRF floor of the ladder (escalation +6/+12, capped at 45)
	VideoVMAFMin float64 // min acceptable VMAF for ESCALATED levels (0 disables the guard)
	Encoder      string
	CPUBudget    int // threads granted to each child encoder/vmaf (0 = codec default)
}

// PhotoResult reports what EncodeImage actually achieved when it kept a temp:
// the effective q of the winning encode (after ladder + guard) and the measured
// VMAF against the original (-1 = never measured). The runner stores it so
// future runs know at which quality the photo already sits.
type PhotoResult struct {
	Q    int
	VMAF float64
}

// PhotoLadder returns the per-file q levels tried within one run: base,
// base-6, base-12 (floored at 61). Mirror image of CRFLadder for stills:
// already-compressed sources (messenger/social photos) cannot shrink at the
// base quality, only deeper levels can beat them honestly.
func PhotoLadder(base int) []int {
	base = clamp(base, 61, 100)
	levels := []int{base}
	for _, step := range []int{6, 12} {
		n := clamp(base-step, 61, 100)
		if n != levels[len(levels)-1] {
			levels = append(levels, n)
		}
	}
	return levels
}

// EncodeImage stills -> AVIF (CPU: avifenc preferred, ffmpeg fallback). GPU is a
// video concept; still-image AVIF stays CPU regardless of --gpu. avifenc only
// decodes JPEG/PNG, so on any avifenc failure (e.g. WebP/HEIC input) we fall
// back to ffmpeg, which decodes everything.
//
// Same per-file quality matching as video: the first ladder level (see
// PhotoLadder) that produces a file smaller than origSize wins. The BASE level
// is taken as-is (it is the user's quality order); an ESCALATED winner is
// verified against the original with libvmaf (single-frame) and, if the
// difference is visible (VMAF < PhotoVMAFMin), we bisection-search the q band
// for a point that is BOTH smaller AND imperceptible. No such point -> the
// file is left untouched (ErrNotSmaller): we never trade visible quality for
// size. Lossless mode has no ladder (quality is fixed by definition).
func EncodeImage(ctx context.Context, loc *tools.Locator, o Opts, src, tmp string, origSize int64) (PhotoResult, error) {
	attempt := func(q int) (kept, ran bool, lastErr error) {
		if err := encodeImageAt(ctx, loc, o, q, src, tmp); err != nil {
			return false, false, err
		}
		ran = true // encoder produced a file successfully
		if st, serr := os.Stat(tmp); serr == nil && st.Size() < origSize {
			return true, true, nil // real win: keep tmp
		}
		os.Remove(tmp) // ran fine but no reduction
		return false, ran, nil
	}
	score := func() float64 {
		ff := loc.FFmpeg()
		if ff == "" {
			return -1 // no ffmpeg for the metric: honest fallback (do not block)
		}
		return vmafScore(ctx, loc, ff, tmp, src, o.CPUBudget)
	}

	if o.PhotoLoss {
		kept, ran, err := attempt(o.PhotoQ)
		if kept {
			return PhotoResult{Q: o.PhotoQ, VMAF: -1}, nil
		}
		if !ran && err != nil {
			return PhotoResult{}, err
		}
		return PhotoResult{}, ErrNotSmaller
	}

	ladder := PhotoLadder(o.PhotoQ)
	anyRan := false
	var lastErr error
	for i, lvl := range ladder {
		kept, ran, err := attempt(lvl)
		anyRan = anyRan || ran
		if err != nil {
			lastErr = err
		}
		if !kept {
			continue
		}
		if i == 0 || o.PhotoVMAFMin <= 0 {
			return PhotoResult{Q: lvl, VMAF: -1}, nil // base level: user's order; or guard disabled
		}
		if s := score(); s < 0 || s >= o.PhotoVMAFMin {
			return PhotoResult{Q: lvl, VMAF: s}, nil // passed the floor (no metric -> honest fallback)
		}
		// Smaller but visibly worse: bisect between this q (small, visible)
		// and the previous higher q (invisible, but it did not shrink).
		os.Remove(tmp)
		lo, hi := lvl, ladder[i-1] // q goes DOWN = more compression
		for probe := 0; probe < 4; probe++ {
			mid := (lo + hi) / 2
			if mid <= lo || mid >= hi {
				break
			}
			k2, ran2, e2 := attempt(mid)
			anyRan = anyRan || ran2
			if e2 != nil {
				lastErr = e2
			}
			if !k2 {
				hi = mid // still not smaller: compress harder
				continue
			}
			if s2 := score(); s2 < 0 || s2 >= o.PhotoVMAFMin {
				return PhotoResult{Q: mid, VMAF: s2}, nil // both smaller and imperceptible
			}
			os.Remove(tmp)
			lo = mid // smaller but visible: pull quality back up
		}
		// No point in the band was both smaller AND imperceptible -> leave
		// the file alone; deeper levels only trade more quality.
		return PhotoResult{}, ErrNotSmaller
	}
	if anyRan {
		return PhotoResult{}, ErrNotSmaller
	}
	if lastErr != nil {
		return PhotoResult{}, lastErr
	}
	return PhotoResult{}, ErrNotSmaller
}

// encodeImageAt encodes one still at quality q (0..100), avifenc first then
// ffmpeg. Same tool-quirk handling as the old single-shot EncodeImage.
func encodeImageAt(ctx context.Context, loc *tools.Locator, o Opts, q int, src, tmp string) error {
	if a := loc.Avifenc(); a != "" {
		args := []string{"--speed", strconv.Itoa(clamp(o.PhotoSpeed, 0, 10))}
		if o.PhotoLoss {
			args = append(args, "--lossless")
		} else {
			args = append(args, "-q", strconv.Itoa(clamp(q, 0, 100)))
		}
		if o.CPUBudget > 0 {
			args = append(args, "-j", strconv.Itoa(o.CPUBudget)) // share cores across workers
		}
		args = append(args, src, tmp)
		if err := run(loc, a, args...); err == nil {
			if st, serr := os.Stat(tmp); serr == nil && st.Size() > 0 {
				return nil
			}
		}
		os.Remove(tmp) // partial output from the failed attempt
	}
	return encodeImageFFmpeg(ctx, loc, o, q, src, tmp)
}

// encodeImageFFmpeg is the fallback: decodes any image via ffmpeg and muxes AVIF.
func encodeImageFFmpeg(ctx context.Context, loc *tools.Locator, o Opts, q int, src, tmp string) error {
	ff := loc.FFmpeg()
	if ff == "" {
		return fmt.Errorf("no encoder: neither avifenc nor ffmpeg found")
	}
	args := []string{
		"-y", "-i", src,
		"-c:v", "libaom-av1", "-still-picture", "1",
		"-crf", strconv.Itoa(aomCRFFromQ(q, o.PhotoLoss)),
		"-pix_fmt", "yuv420p",
		"-map_metadata", "-1",
	}
	if o.CPUBudget > 0 {
		args = append(args, "-threads", strconv.Itoa(o.CPUBudget))
	}
	args = append(args, tmp)
	return run(loc, ff, args...)
}

// EncodeAnimated GIF/animated WebP -> animated AVIF. tmp must end in .avif.
// Tries avifenc first (best animated AVIF; reads GIF natively), then ffmpeg.
func EncodeAnimated(ctx context.Context, loc *tools.Locator, o Opts, src, tmp string) error {
	if a := loc.Avifenc(); a != "" {
		args := []string{"--speed", strconv.Itoa(clamp(o.PhotoSpeed, 0, 10))}
		if o.PhotoLoss {
			args = append(args, "--lossless")
		} else {
			args = append(args, "-q", strconv.Itoa(clamp(o.PhotoQ, 0, 100)))
		}
		if o.CPUBudget > 0 {
			args = append(args, "-j", strconv.Itoa(o.CPUBudget))
		}
		args = append(args, src, tmp)
		if err := run(loc, a, args...); err == nil {
			if st, serr := os.Stat(tmp); serr == nil && st.Size() > 0 {
				return nil
			}
		}
		// fall back to ffmpeg below
	}
	ff := loc.FFmpeg()
	if ff == "" {
		return fmt.Errorf("no encoder for animated AVIF (avifenc failed and ffmpeg not found)")
	}
	args := []string{
		"-y", "-i", src,
		"-c:v", "libaom-av1",
		"-crf", strconv.Itoa(aomCRFFromQ(o.PhotoQ, o.PhotoLoss)),
		"-pix_fmt", "yuv420p",
		"-map_metadata", "-1",
	}
	if o.CPUBudget > 0 {
		args = append(args, "-threads", strconv.Itoa(o.CPUBudget))
	}
	args = append(args, tmp)
	if err := run(loc, ff, args...); err != nil {
		return fmt.Errorf("animated AVIF failed (avifenc + ffmpeg tried): %w", err)
	}
	return nil
}

// ---- video: GPU-first with CPU fallback ----

type cand struct {
	codec string
	flag  string
	value string
}

// VideoChain is the ordered list of AV1 encoders to try for a given mode.
// It remembers the mode/availability it was built from, so the CRF level can
// be re-resolved per file (see EncodeVideo's auto ladder) without re-probing.
type VideoChain struct {
	cands []cand
	mode  string          // encoder mode this chain was built for
	avail map[string]bool // probed encoder availability
	crf   int             // base CRF that produced cands
}

// Codecs returns the codec names in try-order (for logging).
func (v VideoChain) Codecs() []string {
	out := make([]string, len(v.cands))
	for i, c := range v.cands {
		out[i] = c.codec
	}
	return out
}

// WithCRF returns the same encoder order rebuilt at another CRF level.
func (v VideoChain) WithCRF(crf int) VideoChain {
	if crf == v.crf {
		return v
	}
	nc := Opts{Encoder: v.mode, VideoCRF: crf}.buildChain(v.avail)
	nc.mode, nc.avail, nc.crf = v.mode, v.avail, crf
	return nc
}

// CRFLadder returns the per-file CRF levels tried within one run: base,
// base+6, base+12 (capped at 45). The first level that yields a smaller file
// wins; escalation happens only when the lower level could NOT shrink the
// file at all. Measured on the real archive: for socially re-encoded sources
// (already quality-capped by the platform) base+12 lands at VMAF >= 95
// (difference invisible to the eye), while clean camera footage never leaves
// the base level — so this approximates per-file quality matching for free.
func CRFLadder(base int) []int {
	base = clamp(base, 1, 45)
	levels := []int{base}
	for _, step := range []int{6, 12} {
		n := clamp(base+step, 1, 45)
		if n != levels[len(levels)-1] {
			levels = append(levels, n)
		}
	}
	return levels
}

// ProbeAV1Encoders returns codec->usable by ACTUALLY encoding 2 synthetic
// frames with each AV1 encoder that the ffmpeg build lists. Compile-time
// presence (-encoders) is not enough: nvenc/amf/qsv can be built in while the
// GPU lacks AV1 encode support or vendor runtime, and then fail on every file
// mid-run. One real probe per encoder at startup (before workers start) is
// decisive and costs ~2-4s total. Per-file fallback ordering stays as the
// safety net for failures that only appear under load/driver hiccups.
func ProbeAV1Encoders(ctx context.Context, loc *tools.Locator) map[string]bool {
	avail := map[string]bool{}
	ff := loc.FFmpeg()
	if ff == "" {
		return avail
	}
	out, err := loc.Run(ctx, ff, "-hide_banner", "-encoders")
	if err != nil {
		return avail
	}
	// Probe args mirror what buildChain bakes in (vendor rc/quality must be
	// valid for the probe to be fair); fast settings to keep startup cheap.
	probeArgs := map[string][]string{
		"av1_nvenc":  {"-cq", "28"},
		"av1_amf":    {"-rc", "cqp", "-qp_i", "28", "-qp_p", "28"},
		"av1_qsv":    {"-global_quality", "28"},
		"libsvtav1":  {"-crf", "30"},
		"libaom-av1": {"-crf", "30", "-cpu-used", "9"},
	}
	for codec, q := range probeArgs {
		if !strings.Contains(string(out), codec) {
			continue // not even in the build
		}
		pctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		args := append([]string{"-hide_banner", "-loglevel", "error", "-f", "lavfi",
			"-i", "testsrc=s=320x240:r=24", "-frames:v", "2", "-pix_fmt", "yuv420p",
			"-c:v", codec}, q...)
		args = append(args, "-f", "null", "-")
		_, err := loc.Run(pctx, ff, args...)
		cancel()
		if err == nil {
			avail[codec] = true
		}
	}
	return avail
}

// BuildVideoChain resolves the try-order for a mode given encoder availability.
// If the ffmpeg build lacks libsvtav1 (e.g. gyan essentials) but has libaom-av1,
// the CPU slot honestly degrades to libaom instead of failing mid-run.
func (o Opts) BuildVideoChain(avail map[string]bool) VideoChain {
	v := o.buildChain(avail)
	v.mode, v.avail, v.crf = o.Encoder, avail, o.VideoCRF
	return v
}

// buildChain assembles candidate encoders with quality values baked in.
func (o Opts) buildChain(avail map[string]bool) VideoChain {
	crf := o.VideoCRF
	svt := cand{"libsvtav1", "-crf", strconv.Itoa(clamp(crf, 1, 45))}
	aom := cand{"libaom-av1", "-crf", strconv.Itoa(clamp(crf, 4, 40))}
	nv := cand{"av1_nvenc", "-cq", strconv.Itoa(clamp(crf, 8, 45))}
	amf := cand{"av1_amf", "-rc cqp -qp_i -qp_p", fmt.Sprintf("%d %d", clamp(crf, 8, 45), clamp(crf, 8, 45))}
	qsv := cand{"av1_qsv", "-global_quality", strconv.Itoa(clamp(crf, 8, 45))}

	cpu := svt
	if !avail["libsvtav1"] && avail["libaom-av1"] {
		cpu = aom // ffmpeg build without SVT-AV1 (probed) -> honest fallback
	}

	var gpu []cand
	for _, c := range []cand{nv, amf, qsv} {
		if avail[c.codec] {
			gpu = append(gpu, c)
		}
	}

	switch o.Encoder {
	case EncLibaom, "max":
		return VideoChain{cands: []cand{aom}}
	case EncCPU:
		return VideoChain{cands: []cand{cpu}}
	case EncGPU:
		return VideoChain{cands: gpu} // may be empty -> caller reports "no GPU AV1"
	case EncNVENC:
		return VideoChain{cands: []cand{nv}}
	case EncAMF:
		return VideoChain{cands: []cand{amf}}
	case EncQSV:
		return VideoChain{cands: []cand{qsv}}
	default: // auto: GPU first, then CPU (SVT-AV1 or libaom)
		return VideoChain{cands: append(gpu, cpu)}
	}
}

// VideoResult reports what EncodeVideo actually achieved when it kept a temp:
// the effective CRF of the winning encode (after ladder + bisection) and the
// measured VMAF against the original (-1 = never measured). The runner stores
// it so future runs know at which quality the file already sits.
type VideoResult struct {
	CRF  int
	VMAF float64
}

// EncodeVideo tries the encoder chain across the CRF ladder (see CRFLadder):
// the first level that produces a file smaller than origSize wins (so a GPU
// attempt that doesn't help defers to CPU, and a source that can't shrink at
// the base quality is retried at coarser qualities instead of being skipped).
// Only if NO level shrinks it do we return ErrNotSmaller.
//
// Quality guard: the BASE level is taken as-is (it is the user's quality
// order), but an ESCALATED winner is verified against the original with
// libvmaf. If the difference is visible (VMAF < floor) we bisection-search
// the (previous, winner) CRF band for a point that is BOTH smaller than the
// original AND imperceptible; if no such point exists the file is left
// untouched — we never trade visible quality for size.
//
// Audio is copied losslessly; if that fails for ALL encoders on ALL levels
// (audio codec incompatible with MP4, e.g. Opus/Vorbis) it retries the whole
// ladder transcoding audio to AAC so the file is still valid and usually smaller.
func EncodeVideo(ctx context.Context, loc *tools.Locator, o Opts, chain VideoChain, src, tmp string, origSize int64) (VideoResult, error) {
	ff := loc.FFmpeg()
	if ff == "" {
		return VideoResult{}, fmt.Errorf("no encoder: ffmpeg not found for video")
	}
	if len(chain.cands) == 0 {
		return VideoResult{}, fmt.Errorf("no usable AV1 encoder for mode %q", o.Encoder)
	}

	attempt := func(ch VideoChain, audioArgs []string) (kept, ran bool, lastErr error) {
		for _, c := range ch.cands {
			args := videoFFArgs(c, audioArgs, o.CPUBudget, src, tmp)
			if err := run(loc, ff, args...); err != nil {
				lastErr = fmt.Errorf("%s: %w", c.codec, err)
				os.Remove(tmp)
				continue
			}
			ran = true // encoder produced a file successfully
			if st, serr := os.Stat(tmp); serr == nil && st.Size() < origSize {
				return true, true, nil // real win: keep tmp
			}
			os.Remove(tmp) // ran fine but no reduction: try next candidate
		}
		return false, ran, lastErr
	}

	ladder := CRFLadder(chain.crf)

	// runLadder walks the whole ladder for one audio strategy, applying the
	// VMAF guard + bisection to escalated winners (see function doc).
	runLadder := func(audioArgs []string) (kept bool, info VideoResult, anyRan bool, lastErr error) {
		for i, lvl := range ladder {
			kept, ran, err := attempt(chain.WithCRF(lvl), audioArgs)
			if kept {
				if i == 0 || o.VideoVMAFMin <= 0 {
					return true, VideoResult{CRF: lvl, VMAF: -1}, true, nil // base level: user's order; or guard disabled
				}
				score := vmafScore(ctx, loc, ff, tmp, src, o.CPUBudget)
				if score < 0 || score >= o.VideoVMAFMin {
					return true, VideoResult{CRF: lvl, VMAF: score}, true, nil // passed the floor (libvmaf missing -> honest fallback)
				}
				// Smaller but visibly worse: bisection in the band between the
				// last level that could NOT shrink and this level that lost VMAF.
				os.Remove(tmp)
				lo, hi := ladder[i-1], lvl
				for probe := 0; probe < 4; probe++ {
					mid := (lo + hi) / 2
					if mid <= lo || mid >= hi {
						break
					}
					k2, ran2, e2 := attempt(chain.WithCRF(mid), audioArgs)
					anyRan = anyRan || ran2
					if e2 != nil {
						lastErr = e2
					}
					if !k2 {
						lo = mid // still not smaller: push crf up
						continue
					}
					s2 := vmafScore(ctx, loc, ff, tmp, src, o.CPUBudget)
					if s2 < 0 || s2 >= o.VideoVMAFMin {
						return true, VideoResult{CRF: mid, VMAF: s2}, true, nil // both smaller and imperceptible
					}
					os.Remove(tmp)
					hi = mid // smaller but visible: pull crf back
				}
				// No point in the band was both smaller AND imperceptible ->
				// leave the file alone; deeper levels only trade more quality.
				return false, VideoResult{}, true, nil
			}
			anyRan = anyRan || ran
			if err != nil {
				lastErr = err
			}
		}
		return false, VideoResult{}, anyRan, lastErr
	}

	// Pass 1: lossless audio copy across the ladder.
	kept, info, anyRan, lastErr := runLadder([]string{"-c:a", "copy"})
	if kept {
		return info, nil
	}
	if anyRan {
		// Encoders worked (audio copy is fine) but nothing shrank enough
		// honestly (or the guard refused a visible trade) -> honest outcome.
		return VideoResult{}, ErrNotSmaller
	}

	// Pass 2: audio copy failed for every encoder on every level ->
	// container/audio incompatible. Transcode audio to AAC across the ladder.
	kept, info, anyRan, lastErr = runLadder([]string{"-c:a", "aac", "-b:a", "192k"})
	if kept {
		return info, nil
	}
	if anyRan {
		return VideoResult{}, ErrNotSmaller
	}
	if lastErr != nil {
		return VideoResult{}, lastErr
	}
	return VideoResult{}, ErrNotSmaller
}

// vmafScore scores candidate against the original file (reference) with
// libvmaf. Works for videos and single frames (stills). >=95 was measured on
// this archive as "difference invisible to the eye". threads caps libvmaf's
// core use so parallel workers do not oversubscribe the machine. Returns -1
// when libvmaf is unavailable or the pass failed — callers must not block
// compression on a metrics-tool hiccup (honest fallback).
func vmafScore(ctx context.Context, loc *tools.Locator, ff, candidate, original string, threads int) float64 {
	filter := "libvmaf"
	if threads > 0 {
		filter = fmt.Sprintf("libvmaf=n_threads=%d", threads)
	}
	out, err := loc.Run(ctx, ff, "-hide_banner", "-i", candidate, "-i", original,
		"-lavfi", filter, "-f", "null", "-")
	if err != nil {
		return -1
	}
	s := string(out)
	i := strings.LastIndex(s, "VMAF score:")
	if i < 0 {
		return -1
	}
	var score float64
	if _, err := fmt.Sscanf(s[i:], "VMAF score: %f", &score); err != nil {
		return -1
	}
	return score
}

// videoFFArgs assembles an ffmpeg command for one video candidate + audio args.
// threads grants each CPU encoder its slice of the core pool (0 = codec
// default). GPU encoders (nvenc/amf/qsv) ignore it.
func videoFFArgs(c cand, audioArgs []string, threads int, src, tmp string) []string {
	args := []string{
		"-y", "-i", src,
		"-map", "0:v:0",
		"-map", "0:a?",
		"-c:v", c.codec,
	}
	args = append(args, splitQuality(c.flag, c.value)...)
	switch c.codec {
	case "libsvtav1":
		if threads > 0 {
			args = append(args, "-svtav1-params", fmt.Sprintf("lp=%d", threads))
		}
	case "libaom-av1":
		if threads > 0 {
			args = append(args, "-threads", strconv.Itoa(threads))
		}
	}
	args = append(args, audioArgs...)
	args = append(args,
		"-movflags", "+faststart",
		"-map_metadata", "0",
		"-map_chapters", "0",
		tmp,
	)
	return args
}

// splitQuality expands a possibly multi-flag rate-control spec into ffmpeg args.
func splitQuality(flag, value string) []string {
	flags := strings.Fields(flag)
	vals := strings.Fields(value)
	var out []string
	for i, f := range flags {
		out = append(out, f)
		if i < len(vals) {
			out = append(out, vals[i])
		}
	}
	return out
}

// aomCRFFromQ maps AVIF photo-q (0..100, higher better) to libaom -crf (0..63,
// lower better). lossless -> crf 0.
func aomCRFFromQ(q int, lossless bool) int {
	if lossless {
		return 0
	}
	return clamp(63-int(float64(clamp(q, 0, 100))*0.63), 4, 63)
}

func run(loc *tools.Locator, toolPath string, args ...string) error {
	_, err := loc.Run(context.Background(), toolPath, args...)
	return err
}

func clamp(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}
