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
	PhotoQ    int  // 0..100
	PhotoLoss bool // pixel-exact AVIF
	VideoCRF  int  // 0..50 (AV1), lower = better/heavier
	Encoder   string
}

// EncodeImage stills -> AVIF (CPU: avifenc preferred, ffmpeg fallback). GPU is a
// video concept; still-image AVIF stays CPU regardless of --gpu. avifenc only
// decodes JPEG/PNG, so on any avifenc failure (e.g. WebP/HEIC input) we fall
// back to ffmpeg, which decodes everything.
func EncodeImage(ctx context.Context, loc *tools.Locator, o Opts, src, tmp string) error {
	if a := loc.Avifenc(); a != "" {
		args := []string{"--speed", "6"}
		if o.PhotoLoss {
			args = append(args, "--lossless")
		} else {
			args = append(args, "-q", strconv.Itoa(clamp(o.PhotoQ, 0, 100)))
		}
		args = append(args, src, tmp)
		if err := run(loc, a, args...); err == nil {
			if st, serr := os.Stat(tmp); serr == nil && st.Size() > 0 {
				return nil
			}
		}
		os.Remove(tmp) // partial output from the failed attempt
	}
	return encodeImageFFmpeg(ctx, loc, o, src, tmp)
}

// encodeImageFFmpeg is the fallback: decodes any image via ffmpeg and muxes AVIF.
func encodeImageFFmpeg(ctx context.Context, loc *tools.Locator, o Opts, src, tmp string) error {
	ff := loc.FFmpeg()
	if ff == "" {
		return fmt.Errorf("no encoder: neither avifenc nor ffmpeg found")
	}
	args := []string{
		"-y", "-i", src,
		"-c:v", "libaom-av1", "-still-picture", "1",
		"-crf", strconv.Itoa(aomCRFFromQ(o.PhotoQ, o.PhotoLoss)),
		"-pix_fmt", "yuv420p",
		"-map_metadata", "-1",
		tmp,
	}
	return run(loc, ff, args...)
}

// EncodeAnimated GIF/animated WebP -> animated AVIF. tmp must end in .avif.
// Tries avifenc first (best animated AVIF; reads GIF natively), then ffmpeg.
func EncodeAnimated(ctx context.Context, loc *tools.Locator, o Opts, src, tmp string) error {
	if a := loc.Avifenc(); a != "" {
		args := []string{"--speed", "6"}
		if o.PhotoLoss {
			args = append(args, "--lossless")
		} else {
			args = append(args, "-q", strconv.Itoa(clamp(o.PhotoQ, 0, 100)))
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
		tmp,
	}
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
type VideoChain struct{ cands []cand }

// Codecs returns the codec names in try-order (for logging).
func (v VideoChain) Codecs() []string {
	out := make([]string, len(v.cands))
	for i, c := range v.cands {
		out[i] = c.codec
	}
	return out
}

// AvailableEncoders probes ffmpeg for AV1 encoders. Returns codec->available.
func AvailableEncoders(ctx context.Context, loc *tools.Locator) map[string]bool {
	avail := map[string]bool{}
	ff := loc.FFmpeg()
	if ff == "" {
		return avail
	}
	out, err := loc.Run(ctx, ff, "-hide_banner", "-encoders")
	if err != nil {
		return avail
	}
	for _, codec := range []string{"av1_nvenc", "av1_amf", "av1_qsv", "libsvtav1", "libaom-av1"} {
		if strings.Contains(string(out), codec) {
			avail[codec] = true
		}
	}
	return avail
}

// BuildVideoChain resolves the try-order for a mode given encoder availability.
// If the ffmpeg build lacks libsvtav1 (e.g. gyan essentials) but has libaom-av1,
// the CPU slot honestly degrades to libaom instead of failing mid-run.
func (o Opts) BuildVideoChain(avail map[string]bool) VideoChain {
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
		return VideoChain{[]cand{aom}}
	case EncCPU:
		return VideoChain{[]cand{cpu}}
	case EncGPU:
		return VideoChain{gpu} // may be empty -> caller reports "no GPU AV1"
	case EncNVENC:
		return VideoChain{[]cand{nv}}
	case EncAMF:
		return VideoChain{[]cand{amf}}
	case EncQSV:
		return VideoChain{[]cand{qsv}}
	default: // auto: GPU first, then CPU (SVT-AV1 or libaom)
		return VideoChain{append(gpu, cpu)}
	}
}

// EncodeVideo tries the encoder chain, keeping tmp only when it actually shrinks
// the file below origSize (so a GPU attempt that doesn't help defers to CPU).
// If a candidate ran but no encoder beat the original it returns ErrNotSmaller.
// Audio is copied losslessly; if that fails for ALL encoders (audio codec
// incompatible with MP4, e.g. Opus/Vorbis) it retries once transcoding audio to
// AAC so the file is still valid and usually smaller.
func EncodeVideo(ctx context.Context, loc *tools.Locator, o Opts, chain VideoChain, src, tmp string, origSize int64) error {
	ff := loc.FFmpeg()
	if ff == "" {
		return fmt.Errorf("no encoder: ffmpeg not found for video")
	}
	if len(chain.cands) == 0 {
		return fmt.Errorf("no usable AV1 encoder for mode %q", o.Encoder)
	}

	attempt := func(audioArgs []string) (kept, ran bool, lastErr error) {
		for _, c := range chain.cands {
			args := videoFFArgs(c, audioArgs, src, tmp)
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

	// Pass 1: lossless audio copy.
	if kept, ran, err := attempt([]string{"-c:a", "copy"}); kept {
		return nil
	} else if ran {
		// Encoders worked (audio copy is fine) but none shrank it -> honest outcome.
		return ErrNotSmaller
	} else if err == nil {
		return ErrNotSmaller
	}

	// Pass 2: audio copy failed for every encoder -> container/audio incompatible.
	// Transcode audio to AAC and try the chain again.
	if kept, ran, err := attempt([]string{"-c:a", "aac", "-b:a", "192k"}); kept {
		return nil
	} else if ran {
		return ErrNotSmaller
	} else if err != nil {
		return err
	}
	return ErrNotSmaller
}

// videoFFArgs assembles an ffmpeg command for one video candidate + audio args.
func videoFFArgs(c cand, audioArgs []string, src, tmp string) []string {
	args := []string{
		"-y", "-i", src,
		"-map", "0:v:0",
		"-map", "0:a?",
		"-c:v", c.codec,
	}
	args = append(args, splitQuality(c.flag, c.value)...)
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
