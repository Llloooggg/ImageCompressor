// Package runner wires everything into the crash-safe, idempotent pipeline:
// recover interrupted swaps -> clean temp -> walk -> detect -> skip-if-processed
// -> encode -> transfer metadata -> journal+atomic swap -> commit -> stats.
package runner

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/Llloooggg/ImageCompressor/internal/config"
	"github.com/Llloooggg/ImageCompressor/internal/detect"
	"github.com/Llloooggg/ImageCompressor/internal/media"
	"github.com/Llloooggg/ImageCompressor/internal/meta"
	"github.com/Llloooggg/ImageCompressor/internal/safefs"
	"github.com/Llloooggg/ImageCompressor/internal/store"
	"github.com/Llloooggg/ImageCompressor/internal/tools"
	"github.com/Llloooggg/ImageCompressor/internal/vt"

	"golang.org/x/sync/errgroup"
)

const toolVersion = "0.1.0"
const toolSelf = "mediacompressor"

type Runner struct {
	cfg         *config.Config
	store       *store.Store
	loc         *tools.Locator
	opts        media.Opts
	vchain      media.VideoChain
	exif        string
	legacy      map[string]struct{}
	processRoot string
	hasAvifenc  bool
	hasFFmpeg   bool
	changes     *changeLog

	processed, skipDone, skipNotSmaller, skipUnknown, fixedExt, errored atomic.Int64
	origBytes, newBytes, scanBytes                                      atomic.Int64
	done                                                                atomic.Int64
	total                                                               int

	multi     bool        // VT fixed-height status block: counters + per-worker rows
	firstDraw atomic.Bool // first VT repaint of this run
	statusVis bool        // live block is currently on screen (guarded by statusMu)
	statusMu  sync.Mutex  // one repaint at a time (ticker goroutine vs workers)
	activeMu  sync.Mutex
	active    []activeJob // files the workers are chewing on right now
}

// activeJob is one file a worker is processing right now, for the live
// status line ("which files are being worked on this second, how long").
type activeJob struct {
	rel   string
	start time.Time
}

func New(cfg *config.Config, st *store.Store, loc *tools.Locator) *Runner {
	return &Runner{
		cfg:   cfg,
		store: st,
		loc:   loc,
		opts: media.Opts{
			PhotoQ:       cfg.PhotoQ,
			PhotoLoss:    cfg.PhotoLoss,
			PhotoSpeed:   cfg.PhotoSpeed,
			PhotoVMAFMin: cfg.PhotoVMAF,
			VideoCRF:     cfg.VideoCRF,
			VideoVMAFMin: cfg.VideoVMAF,
			Encoder:      cfg.VideoEnc,
			// Auto core split: every child encoder/vmaf gets its slice instead
			// of greedily grabbing all cores (N workers × all-cores children just
			// thrash each other). Thread count does NOT change encoder output
			// (aom/svt/libvmaf are thread-deterministic), so the nochange
			// stamps deliberately exclude it.
			CPUBudget: childBudget(cfg.Workers),
		},
		exif:       loc.Exiftool(),
		hasAvifenc: loc.Avifenc() != "",
		hasFFmpeg:  loc.FFmpeg() != "",
	}
}

// childBudget = cores per worker (floor 1): the auto-computed slice of the
// machine granted to each parallel encoder job.
func childBudget(workers int) int {
	if workers < 1 {
		workers = 1
	}
	t := runtime.NumCPU() / workers
	if t < 1 {
		t = 1
	}
	return t
}

// stampFor fingerprints the settings that decide "can this shrink?". A nochange
// record with a matching stamp is skipped on later runs; a different stamp makes
// us re-evaluate (so changing --photo-q/--photo-vmaf-min/--video-crf/encoder
// takes effect).
func (r *Runner) stampFor(k detect.Kind) string {
	switch k {
	case detect.KindVideo:
		return fmt.Sprintf("vid|%d|%.0f|%s|ff=%v|still=%v", r.opts.VideoCRF, r.opts.VideoVMAFMin, r.opts.Encoder, r.hasFFmpeg, r.cfg.Still)
	case detect.KindAnimated:
		return fmt.Sprintf("anim|%d|%v|avifenc=%v|ff=%v", r.opts.PhotoQ, r.opts.PhotoLoss, r.hasAvifenc, r.hasFFmpeg)
	default:
		return fmt.Sprintf("img|%d|%.0f|%v|avifenc=%v|ff=%v", r.opts.PhotoQ, r.opts.PhotoVMAFMin, r.opts.PhotoLoss, r.hasAvifenc, r.hasFFmpeg)
	}
}

// Recover finishes or discards swaps interrupted by a crash. Run before walking.
func (r *Runner) Recover(ctx context.Context) error {
	rows, err := r.store.PendingStaged(ctx)
	if err != nil {
		return err
	}
	for _, s := range rows {
		finalOK := false
		if _, err := os.Stat(s.FinalPath); err == nil {
			if h, err := hashFile(s.FinalPath); err == nil && h == s.ExpectedHash {
				finalOK = true // swap already landed on disk, just not committed in DB
			}
		}
		switch {
		case finalOK:
			if err := r.finalize(ctx, s.ID, s.FinalPath, s.ExpectedHash); err != nil {
				return err
			}
		case fileExists(s.TmpPath):
			if h, err := hashFile(s.TmpPath); err == nil && h == s.ExpectedHash {
				// rename never happened — finish it safely
				if err := safefs.AtomicReplace(s.TmpPath, s.FinalPath); err != nil {
					return err
				}
				if err := r.finalize(ctx, s.ID, s.FinalPath, s.ExpectedHash); err != nil {
					return err
				}
			} else {
				safefs.RemoveIfExists(s.TmpPath) // garbage
				_ = r.store.DropStaged(ctx, s.ID)
			}
		default:
			_ = r.store.DropStaged(ctx, s.ID) // nothing landed; file will be reprocessed
		}
	}
	return nil
}

// CleanTemps removes leftover temp files under the process root.
func (r *Runner) CleanTemps() error {
	if _, err := os.Stat(r.processRoot); err != nil {
		return nil // fresh --output dir doesn't exist yet — nothing to clean
	}
	_, err := safefs.CleanupTemps(r.processRoot)
	return err
}

// Run executes the full pipeline.
func (r *Runner) Run(ctx context.Context) error {
	r.processRoot = r.cfg.Input
	if !r.cfg.InPlace() {
		r.processRoot = r.cfg.Output
	}

	if err := r.Recover(ctx); err != nil {
		return fmt.Errorf("recover: %w", err)
	}
	if err := r.CleanTemps(); err != nil {
		r.logf("cleanup temps: %v", err)
	}

	if !r.cfg.DryRun {
		// Audit trail next to the DB: what became what, sizes, quality achieved.
		p := changeLogPath(r.cfg.Database)
		cl, err := openChangeLog(p)
		if err != nil {
			log.Printf("changelog %s: %v", p, err)
		} else {
			r.changes = cl
			defer cl.close()
			fmt.Printf("Log: %s\n", p)
		}
	}

	if r.cfg.Migrate {
		lu, err := r.store.LegacyUnmigratedHashes(ctx)
		if err != nil {
			return fmt.Errorf("legacy set: %w", err)
		}
		r.legacy = lu
	}

	// Probe AV1 encoders once and resolve the GPU->CPU fallback chain.
	avail := media.ProbeAV1Encoders(ctx, r.loc)
	r.vchain = r.opts.BuildVideoChain(avail)
	fmt.Printf("Video encoders, mode %q: %s\n", r.opts.Encoder, chainString(r.vchain))
	if r.cfg.Still {
		fmt.Println("Still-image video collapse: on, one static frame with a soundtrack becomes a photo, use --no-still to keep such files as videos")
	} else {
		fmt.Println("Still-image video collapse: off, hat-shaped videos stay normal videos")
	}

	files, err := r.gather(ctx)
	if err != nil {
		return err
	}
	r.total = len(files)
	fmt.Printf("Files found: %d\n", r.total)

	// Live status (progress, savings, ETA) in a real console; when output is
	// redirected/piped, print a compact line every 1000 files so logs stay
	// readable. dry-run forces single-line mode: its per-file lines stream
	// through printBreak and a fixed-height block would only get in the way.
	start := time.Now()
	tty := isTerminal(os.Stdout)
	r.multi = tty && !r.cfg.DryRun && vt.Enable(os.Stdout)
	r.firstDraw.Store(true)
	statusStop := make(chan struct{})
	var statusWg sync.WaitGroup
	if tty {
		statusWg.Add(1)
		go func() {
			defer statusWg.Done()
			tick := time.NewTicker(500 * time.Millisecond)
			defer tick.Stop()
			for {
				select {
				case <-tick.C:
					r.printStatus(start)
				case <-statusStop:
					return
				}
			}
		}()
	}

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(r.cfg.Workers)
	for _, f := range files {
		f := f
		g.Go(func() error {
			if gctx.Err() != nil {
				return nil // stop requested: don't start new files
			}
			r.trackStart(f)
			r.process(gctx, f)
			r.trackEnd(f)
			n := r.done.Add(1)
			if tty {
				r.printStatus(start)
			} else if n%1000 == 0 {
				fmt.Printf("Processed: %d/%d\n", n, r.total)
			}
			return nil
		})
	}
	err = g.Wait()
	close(statusStop)
	statusWg.Wait()
	if r.multi && r.statusVis {
		// cursor sits at the end of the block's last row: step up to the
		// first row, then clear every row of the fixed-height block.
		n := r.blockLines()
		fmt.Printf("\x1b[%dA", n-1)
		for i := 0; i < n; i++ {
			fmt.Print("\x1b[2K")
			if i < n-1 {
				fmt.Print("\n")
			}
		}
		fmt.Print("\r")
	} else if tty && !r.multi {
		fmt.Print("\r" + strings.Repeat(" ", 110) + "\r")
	}
	if err != nil {
		return err
	}
	fmt.Println()
	return nil
}

// printStatus renders the live status (TTY mode). Labels stay terse/ASCII for
// codepage safety. With VT it is a FIXED-HEIGHT block so nothing jumps: a
// counter header, one row per worker slot (elapsed time in a fixed column
// first, then the relative path — paths are not clipped unless they would
// exceed the console width and wrap, which would corrupt the cursor math),
// then a last-finished row. Percent is TRUNCATED so it never reads 100% while
// any worker still runs.
func (r *Runner) printStatus(start time.Time) {
	r.statusMu.Lock()
	defer r.statusMu.Unlock()
	done := r.done.Load()
	pct := 0
	if r.total > 0 {
		pct = int(100 * done / int64(r.total))
	}
	el := time.Since(start)
	savedMB := float64(r.origBytes.Load()-r.newBytes.Load()) / (1024 * 1024)
	// ETA from scanned bytes already accounted for (file-count ETA is skewed
	// by instant skip-hits; bytes track real work much better).
	eta := "--:--:--"
	doneB, scanB := r.origBytes.Load(), r.scanBytes.Load()
	if doneB > 0 && scanB > doneB {
		eta = fmtDur(time.Duration(float64(el) * float64(scanB-doneB) / float64(doneB)))
	}
	header := fmt.Sprintf("[%s] %d/%d (%d%%)  ok:%d already:%d nofit:%d ext:%d err:%d  |  saved %.0f MB  |  ETA %s",
		fmtDur(el), done, r.total, pct,
		r.processed.Load(), r.skipDone.Load(), r.skipNotSmaller.Load(),
		r.fixedExt.Load(), r.errored.Load(),
		savedMB, eta)
	if !r.multi {
		fmt.Print("\r" + fmt.Sprintf("%-110s", header))
		return
	}

	width := vt.Width(os.Stdout)
	if width <= 0 {
		width = 100 // unknown console: stay narrow so a row never wraps
	}
	lines := r.statusBlock(clip(header, width-4), width-1)
	n := len(lines)
	if r.firstDraw.Swap(false) {
		for i, ln := range lines {
			if i > 0 {
				fmt.Print("\n")
			}
			fmt.Print(ln)
		}
	} else {
		// cursor sits at the end of the previous block's last row: step up to
		// the first row, then clear+rewrite each row in place.
		fmt.Printf("\x1b[%dA", n-1)
		for i, ln := range lines {
			if i > 0 {
				fmt.Print("\n")
			}
			fmt.Print("\x1b[2K\r" + ln)
		}
	}
	r.statusVis = true
}

// statusBlock builds the fixed-height live block: header + one row per worker
// slot (always r.cfg.Workers rows, idle slots show dashes). maxw clips a row
// to the console width (rune-safe) so it never wraps.
func (r *Runner) statusBlock(header string, maxw int) []string {
	workers := r.cfg.Workers
	if workers < 1 {
		workers = 1
	}
	r.activeMu.Lock()
	jobs := make([]activeJob, len(r.active))
	copy(jobs, r.active)
	r.activeMu.Unlock()

	lines := make([]string, 0, workers+1)
	lines = append(lines, header)
	// clip budget leaves room for clip's ellipsis so the row (with "...")
	// still fits the console width and never wraps.
	clipw := maxw - 3
	if clipw < 8 {
		clipw = 8
	}
	now := time.Now()
	for i := 0; i < workers; i++ {
		if i < len(jobs) {
			j := jobs[i]
			lines = append(lines, clip(fmt.Sprintf("%2d  %8s  %s", i+1, fmtDurShort(now.Sub(j.start)), j.rel), clipw))
		} else {
			lines = append(lines, fmt.Sprintf("%2d  %8s", i+1, "--"))
		}
	}
	return lines
}

// fmtDurShort renders an in-flight elapsed time compactly: "45s" under a
// minute, mm:ss with a padded first digit up to an hour (the battle complaint:
// "1840s" is unreadable), h:mm:ss beyond. Right-align with %8s in the block.
func fmtDurShort(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	s := int(d.Seconds())
	if s < 60 {
		return fmt.Sprintf("%ds", s)
	}
	if s < 3600 {
		return fmt.Sprintf("%d:%02d", s/60, s%60)
	}
	return fmt.Sprintf("%d:%02d:%02d", s/3600, (s%3600)/60, s%60)
}

// blockLines is the fixed row count of the live block for the current worker
// count (header + workers slots).
func (r *Runner) blockLines() int {
	w := r.cfg.Workers
	if w < 1 {
		w = 1
	}
	return w + 1
}

// trackStart/trackEnd maintain the live list of in-flight files (only when
// the two-line status is on; skips are tracked too, they are cheap).
func (r *Runner) trackStart(path string) {
	if !r.multi {
		return
	}
	r.activeMu.Lock()
	r.active = append(r.active, activeJob{rel: r.rel(path), start: time.Now()})
	r.activeMu.Unlock()
}

func (r *Runner) trackEnd(path string) {
	if !r.multi {
		return
	}
	rel := r.rel(path)
	r.activeMu.Lock()
	for i := range r.active {
		if r.active[i].rel == rel {
			r.active = append(r.active[:i], r.active[i+1:]...)
			break
		}
	}
	r.activeMu.Unlock()
}

// clip truncates s to max bytes on a UTF-8 rune boundary (names in the
// archive are Cyrillic/emoji; cutting mid-rune would print garbage).
func clip(s string, max int) string {
	if len(s) <= max {
		return s
	}
	t := s[:max]
	for len(t) > 0 && !utf8.ValidString(t) {
		t = t[:len(t)-1]
	}
	return t + "..."
}

func fmtDur(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	return fmt.Sprintf("%02d:%02d:%02d", int(d.Hours()), int(d.Minutes())%60, int(d.Seconds())%60)
}

func isTerminal(f *os.File) bool {
	fi, err := f.Stat()
	if err != nil {
		return false
	}
	return fi.Mode()&os.ModeCharDevice != 0
}

// gather lists files to process (copying to --output first when needed).
func (r *Runner) gather(ctx context.Context) ([]string, error) {
	src := r.cfg.Input
	// Guard: the output dir may live inside the input dir — never walk into it
	// (previous runs' mirrors would resurrect as inputs).
	outDir := ""
	if !r.cfg.InPlace() && isUnder(r.cfg.Output, src) {
		outDir = r.cfg.Output
	}
	var out []string
	err := filepath.WalkDir(src, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return nil // don't abort on a single unreadable entry
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if outDir != "" && (p == outDir || strings.HasPrefix(p, outDir+string(filepath.Separator))) {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if d.IsDir() {
			return nil
		}
		if safefs.IsTempName(p) || isOurArtifact(p, r.cfg.Database) {
			return nil
		}
		if fi, ierr := d.Info(); ierr == nil {
			r.scanBytes.Add(fi.Size()) // feeds the byte-based ETA in the live status
		}
		out = append(out, p)
		return nil
	})
	if err != nil {
		return nil, err
	}

	if r.cfg.InPlace() {
		return out, nil
	}

	// Output mode: the output dir must be a FULL mirror of the input — every
	// file lands there (non-media copied as-is, media copied then processed
	// in place inside the mirror; originals never touched).
	var dest []string
	for _, p := range out {
		rel, err := filepath.Rel(src, p)
		if err != nil {
			continue
		}
		dpath := filepath.Join(r.cfg.Output, rel)

		// Incremental: the honest final output already exists (swaps are
		// atomic renames) — re-copying would resurrect raw originals.
		if res, derr := detect.Detect(p); derr == nil && res.Kind != detect.KindUnknown {
			if final := r.finalPath(dpath, res); fileExists(final) {
				r.skipDone.Add(1)
				r.addSize(p, final)
				continue
			}
			// A previous run mirrored this file but fixed a lying extension:
			// reuse the honest copy instead of re-copying the raw liar.
			if honest := res.Format.HonestExt(); honest != "" {
				if hp := replaceExt(dpath, honest); hp != dpath && fileExists(hp) && sameSize(p, hp) {
					dest = append(dest, hp)
					continue
				}
			}
		}
		// Already mirrored (same size) by a previous run — reuse it instead of
		// re-copying bytes every run (media swaps always change the size).
		if sameSize(p, dpath) {
			dest = append(dest, dpath)
			continue
		}
		if err := os.MkdirAll(filepath.Dir(dpath), 0o755); err != nil {
			return nil, err
		}
		if err := safefs.CopyFileBytes(p, dpath); err != nil {
			r.logf("copy %s: %v", p, err)
			continue
		}
		// Output mode: fs times must mirror the originals (rule: preserve
		// timestamps). CopyFileBytes moves bytes only; without this the swaps
		// below would propagate the fresh (copy-time) stamps instead.
		if err := meta.CopyFileTimes(p, dpath); err != nil {
			r.logf("copy times %s: %v", rel, err)
		}
		if err := meta.CopyBirthTime(p, dpath); err != nil {
			r.logf("copy birthtime %s: %v", rel, err)
		}
		dest = append(dest, dpath)
	}
	return dest, nil
}

// isUnder reports whether child is parent itself or lives inside it.
func isUnder(child, parent string) bool {
	rel, err := filepath.Rel(parent, child)
	if err != nil {
		return false
	}
	return rel == "." || rel == string(filepath.Separator) ||
		(rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)))
}

// sameSize compares file sizes; false if either is missing/unreadable.
func sameSize(a, b string) bool {
	ai, err := os.Stat(a)
	if err != nil {
		return false
	}
	bi, err := os.Stat(b)
	if err != nil {
		return false
	}
	return ai.Size() == bi.Size()
}

func (r *Runner) process(ctx context.Context, path string) {
	if ctx.Err() != nil {
		return // stop requested: leave the file untouched for the next run
	}
	res, err := detect.Detect(path)
	if err != nil {
		r.errored.Add(1)
		r.logf("detect %s: %v", path, err)
		return
	}
	if res.Kind == detect.KindUnknown {
		r.skipUnknown.Add(1)
		return
	}

	if r.cfg.FixExt {
		r.fixExtOnly(path, res)
		return
	}

	h, err := hashFile(path)
	if err != nil {
		r.errored.Add(1)
		r.logf("hash %s: %v", path, err)
		return
	}

	// Idempotency: decide using the recorded outcome + settings stamp.
	rec, processed, err := r.store.IsProcessed(ctx, h)
	if err != nil {
		r.errored.Add(1)
		r.logf("db lookup %s: %v", path, err)
		return
	}
	if processed {
		switch {
		case rec.Outcome == store.OutcomeNoChange:
			// Could not shrink before. Same settings -> skip; changed settings -> re-evaluate.
			if rec.Stamp == r.stampFor(res.Kind) {
				r.skipDone.Add(1)
				r.addSize(path, path)
				r.ensureHonestExt(path, res) // lying ext is fixed even for untouched files
				return
			}
		default: // compressed / legacy output already on disk
			if !r.legacyNeedsMigrate(h) {
				r.skipDone.Add(1)
				r.addSize(path, path)
				r.ensureHonestExt(path, res)
				return
			}
		}
	}

	// "Hat" detection: a social-media video that is really one static frame
	// with a soundtrack glued on collapses into a photo (see media.
	// StillVideo). Runs after idempotency checks so processed/skipped files
	// never pay for probing; both witnesses must agree before we collapse.
	still := res.Kind == detect.KindVideo && r.cfg.Still && r.hasFFmpeg &&
		media.StillVideo(ctx, r.loc, path)

	rel := r.rel(path)

	// Dry run: just report what would change.
	if r.cfg.DryRun {
		target := filepath.Base(r.finalPath(path, res))
		note := ""
		if still {
			target = replaceExt(target, detect.FmtAVIF.HonestExt())
			note = ", collapse still-image video to photo"
		}
		r.printBreak(func() { fmt.Printf("[dry-run] %s -> %s%s\n", rel, target, note) })
		return
	}

	r.encodeSwap(ctx, path, rel, h, res, still)
}

// legacyNeedsMigrate: in --migrate mode, a legacy-python output that hasn't been
// unified yet should be re-encoded once to the new canon.
func (r *Runner) legacyNeedsMigrate(h string) bool {
	if !r.cfg.Migrate {
		return false
	}
	_, ok := r.legacy[h]
	return ok
}

func (r *Runner) encodeSwap(ctx context.Context, path, rel, srcHash string, res detect.Result, still bool) {
	t0 := time.Now() // per-file wall time -> journal Seconds column (how long this file took)
	if still {
		// The file collapses video -> photo: everything downstream (target
		// extension, DB kind/format, achieved string, journal) follows the
		// image branch. The original file on disk is still the MP4 at this
		// point; media.CollapseStill extracts the frame itself.
		res.Kind = detect.KindImage
		res.Format = detect.FmtAVIF
	}
	finalPath := r.finalPath(path, res) // honest extension target
	// NTFS creation time must be captured BEFORE the swap: when finalPath ==
	// path (same-name replace, e.g. mp4->mp4) the original is gone by the
	// time we would otherwise read it back (the swap left the temp's birth).
	srcBirth, _ := meta.BirthTimeOf(path)
	tmp, err := safefs.TempName(finalPath)
	if err != nil {
		r.errored.Add(1)
		r.logf("temp %s: %v", path, err)
		return
	}

	var encErr error
	var vinfo media.VideoResult
	var pinfo media.PhotoResult
	var origSize int64
	if oi, err := os.Stat(path); err == nil {
		origSize = oi.Size()
	}
	switch res.Kind {
	case detect.KindImage:
		if still {
			pinfo, encErr = media.CollapseStill(ctx, r.loc, r.opts, path, tmp, origSize)
		} else {
			pinfo, encErr = media.EncodeImage(ctx, r.loc, r.opts, path, tmp, origSize)
		}
	case detect.KindAnimated:
		encErr = media.EncodeAnimated(ctx, r.loc, r.opts, path, tmp)
	case detect.KindVideo:
		vinfo, encErr = media.EncodeVideo(ctx, r.loc, r.opts, r.vchain, path, tmp, origSize)
	default:
		encErr = errors.New("unsupported media kind")
	}
	if ctx.Err() != nil {
		// A stop request interrupted the encoder: not an error. Remove the
		// partial temp; the original stays untouched, next run reprocesses it.
		safefs.RemoveIfExists(tmp)
		return
	}
	achieved := r.achievedFor(res.Kind, vinfo, pinfo)
	if still {
		achieved = "still " + achieved // audit: this "photo" was once a video hat
	}
	encNote := attemptsComment(vinfo, pinfo) // per-attempt audit -> journal Comment
	if errors.Is(encErr, media.ErrNotSmaller) {
		safefs.RemoveIfExists(tmp)
		r.skipNotSmaller.Add(1)
		r.addSize(path, path)
		_ = r.store.MarkNoChange(ctx, srcHash, rel, res.Kind.String(), mediaFormatFor(res, finalPath), toolSelf, toolVersion, r.stampFor(res.Kind))
		r.changes.record("not smaller", rel, rel, origSize, origSize, time.Since(t0).Seconds(), res.Kind.String(), mediaFormatFor(res, finalPath), "", encNote)
		r.ensureHonestExt(path, res)
		return
	}
	if encErr != nil {
		safefs.RemoveIfExists(tmp)
		r.errored.Add(1)
		r.logf("encode %s: %v", rel, encErr)
		return
	}

	// Transfer metadata into the temp (so the final file carries the originals).
	// A real exiftool failure ABORTS the swap (temp dropped, original intact,
	// err counted): metadata preservation is an invariant, and we never
	// destroy the original until every invariant step succeeded. Next run
	// retries. ErrNoExiftool stays a warning: a deployment without exiftool
	// still compresses, that is not the file's fault and not worth an error
	// storm across every file.
	if err := meta.TransferTags(ctx, r.exif, path, tmp); err != nil {
		if errors.Is(err, meta.ErrNoExiftool) {
			r.logf("meta: exiftool not found, metadata may be lost: %s", rel)
		} else {
			safefs.RemoveIfExists(tmp)
			r.errored.Add(1)
			r.logf("meta %s: %v (not swapped, original intact, will retry next run)", rel, err)
			return
		}
	}
	// Restore original filesystem times before the swap.
	_ = meta.CopyFileTimes(path, tmp)

	if err := safefs.SyncFile(tmp); err != nil {
		safefs.RemoveIfExists(tmp)
		r.errored.Add(1)
		r.logf("fsync %s: %v", rel, err)
		return
	}
	newHash, err := hashFile(tmp)
	if err != nil {
		safefs.RemoveIfExists(tmp)
		r.errored.Add(1)
		r.logf("hash tmp %s: %v", rel, err)
		return
	}

	// "Smaller — or we don't touch."
	newInfo, _ := os.Stat(tmp)
	origInfo, _ := os.Stat(path)
	if newInfo != nil && origInfo != nil && newInfo.Size() >= origInfo.Size() {
		safefs.RemoveIfExists(tmp)
		r.skipNotSmaller.Add(1)
		r.addSize(path, path)
		_ = r.store.MarkNoChange(ctx, srcHash, rel, res.Kind.String(), mediaFormatFor(res, finalPath), toolSelf, toolVersion, r.stampFor(res.Kind))
		r.changes.record("not smaller", rel, rel, origInfo.Size(), newInfo.Size(), time.Since(t0).Seconds(), res.Kind.String(), mediaFormatFor(res, finalPath), "", encNote)
		r.ensureHonestExt(path, res)
		return
	}

	// Journal, then atomic replace, then commit — recoverable at every step.
	sid, err := r.store.BeginSwap(ctx, srcHash, newHash, finalPath, tmp)
	if err != nil {
		safefs.RemoveIfExists(tmp)
		r.errored.Add(1)
		r.logf("begin swap %s: %v", rel, err)
		return
	}
	if err := safefs.AtomicReplace(tmp, finalPath); err != nil {
		_ = r.store.DropStaged(ctx, sid)
		safefs.RemoveIfExists(tmp)
		r.errored.Add(1)
		r.logf("rename %s: %v", rel, err)
		return
	}
	newRel := r.rel(finalPath)
	// Commit must land even if a stop request arrives right here: the rename
	// already happened, so the DB record has to follow it (WithoutCancel).
	if err := r.store.CommitSwap(context.WithoutCancel(ctx), sid, newHash, newRel, res.Kind.String(), mediaFormatFor(res, finalPath), toolSelf, toolVersion, achieved); err != nil {
		r.errored.Add(1)
		r.logf("commit %s: %v", rel, err)
		return
	}
	if r.cfg.Migrate && r.legacyNeedsMigrate(srcHash) {
		_ = r.store.MarkMigrated(ctx, srcHash)
	}

	// Restore NTFS creation time onto the swapped-in file (Windows; no-op elsewhere).
	// Uses the pre-swap capture so same-name swaps (mp4->mp4) stay honest too.
	if err := meta.SetBirthTime(finalPath, srcBirth); err != nil {
		r.logf("birthtime %s: %v", newRel, err)
	}

	// If we converted a file to a new extension, drop the old one (rename already
	// replaced same-name; a different honest name needs the old removed).
	// Sizes must be tallied BEFORE removing the source (its stat would fail after).
	r.processed.Add(1)
	if origInfo != nil && newInfo != nil {
		r.origBytes.Add(origInfo.Size())
		r.newBytes.Add(newInfo.Size())
	} else {
		r.addSize(path, finalPath)
	}
	dstSize := int64(0)
	if newInfo != nil {
		dstSize = newInfo.Size()
	}
	srcSize := origSize
	if origInfo != nil {
		srcSize = origInfo.Size()
	}
	action := "compressed"
	if still {
		action = "still collapsed" // was a video hat: one frame kept, soundtrack dropped
	}
	r.changes.record(action, rel, newRel, srcSize, dstSize, time.Since(t0).Seconds(), res.Kind.String(), mediaFormatFor(res, finalPath), achieved, encNote)
	if finalPath != path {
		safefs.RemoveIfExists(path)
	}
}

// ensureHonestExt renames a media file whose extension lies, EVEN when the
// file is not compressed (user rule: lying extensions are always fixed —
// viewers must never be handed a file whose bytes contradict its name).
// Doubled/chained media extensions ("VID_x.mp4.mp4", "clip.mp4.mov" — noise
// left by earlier tools' renames) are cleaned up the same way.
// On a name collision the file is left untouched with a warning. DB
// decisions are hash-based, so no stored-filename rewrite is required.
// Call AFTER addSize (a rename would make the old path unstatable).
func (r *Runner) ensureHonestExt(path string, res detect.Result) {
	if !res.ExtensionLies && !doubledMediaExt(path) {
		return
	}
	honest := res.Format.HonestExt()
	if honest == "" {
		return
	}
	newPath := replaceExt(path, honest)
	if newPath == path {
		return
	}
	if r.cfg.DryRun {
		r.printBreak(func() { fmt.Printf("[dry-run] fix-ext: %s -> %s\n", r.rel(path), r.rel(newPath)) })
		return
	}
	if fileExists(newPath) {
		r.logf("fix-ext %s: target %s already exists, keeping name", r.rel(path), r.rel(newPath))
		return
	}
	var sz int64
	if fi, err := os.Stat(path); err == nil {
		sz = fi.Size()
	}
	if err := os.Rename(path, newPath); err != nil {
		r.errored.Add(1)
		r.logf("fix-ext %s: %v", r.rel(path), err)
		return
	}
	r.fixedExt.Add(1)
	r.changes.record("ext renamed", r.rel(path), r.rel(newPath), sz, sz, 0, res.Kind.String(), honest, "", "")
}

func (r *Runner) fixExtOnly(path string, res detect.Result) {
	if !res.ExtensionLies {
		return
	}
	honest := res.Format.HonestExt()
	if honest == "" {
		return
	}
	newPath := replaceExt(path, honest)
	if r.cfg.DryRun {
		r.printBreak(func() { fmt.Printf("[dry-run] fix-ext: %s -> %s\n", r.rel(path), r.rel(newPath)) })
		return
	}
	if err := os.Rename(path, newPath); err != nil {
		r.errored.Add(1)
		r.logf("fix-ext %s: %v", r.rel(path), err)
		return
	}
	// update stored filename mapping for the known hash
	if h, err := hashFile(newPath); err == nil {
		_ = r.store.MarkCompressed(context.Background(), h, r.rel(newPath), res.Kind.String(), string(res.Format), "fix-ext", toolVersion, "")
	}
	r.processed.Add(1)
	r.addSize(path, newPath)
}

func (r *Runner) finalize(ctx context.Context, sid int64, finalPath, expectedHash string) error {
	res, err := detect.Detect(finalPath)
	if err != nil {
		return r.store.DropStaged(ctx, sid)
	}
	return r.store.CommitSwap(ctx, sid, expectedHash, r.rel(finalPath), res.Kind.String(), mediaFormatFor(res, finalPath), toolSelf, toolVersion, "")
}

func (r *Runner) rel(p string) string {
	rel, err := filepath.Rel(r.processRoot, p)
	if err != nil {
		return p
	}
	return rel
}

// finalPath returns the target path with the honest canonical extension.
func (r *Runner) finalPath(path string, res detect.Result) string {
	switch res.Kind {
	case detect.KindVideo:
		return replaceExt(path, ".mp4")
	case detect.KindImage, detect.KindAnimated:
		return replaceExt(path, ".avif")
	default:
		return path
	}
}

func (r *Runner) addSize(origPath, newPath string) {
	if fi, err := os.Stat(origPath); err == nil {
		r.origBytes.Add(fi.Size())
	}
	if fi, err := os.Stat(newPath); err == nil {
		r.newBytes.Add(fi.Size())
	}
}

func (r *Runner) Summary() string {
	ob := float64(r.origBytes.Load()) / 1024 / 1024
	nb := float64(r.newBytes.Load()) / 1024 / 1024
	pct := 0.0
	if ob > 0 {
		pct = (1 - nb/ob) * 100
	}
	return fmt.Sprintf(
		"Done. Compressed: %d, already processed: %d, not smaller: %d, extensions fixed: %d, non-media: %d, errors: %d\n"+
			"Size: %.2f MB -> %.2f MB = %.2f%% saved",
		r.processed.Load(), r.skipDone.Load(), r.skipNotSmaller.Load(), r.fixedExt.Load(), r.skipUnknown.Load(), r.errored.Load(),
		ob, nb, pct,
	)
}

func chainString(v media.VideoChain) string {
	c := v.Codecs()
	if len(c) == 0 {
		return "no usable AV1 encoder, check ffmpeg/drivers"
	}
	return strings.Join(c, " -> ")
}

func mediaFormatFor(res detect.Result, finalPath string) string {
	switch res.Kind {
	case detect.KindVideo:
		return string(detect.FmtMP4)
	case detect.KindImage, detect.KindAnimated:
		return string(detect.FmtAVIF)
	default:
		return filepath.Ext(finalPath)
	}
}

// replaceExt swaps the file's extension for ext, dropping the current one AND
// any chained media extensions under it: "clip.mp4.mov" -> "clip"+ext (never
// "clip.mp4"+ext), "liar.dat" -> "liar"+ext. Dots inside name segments
// ("bandicam 2021-07-21 22-08-04-365", "photo.final") are left intact.
func replaceExt(path, ext string) string {
	base := path
	if e := filepath.Ext(base); e != "" { // drop the current (lying or canonical) ext
		if b := base[:len(base)-len(e)]; b != "" {
			base = b
		}
	}
	for { // drop chained leftovers like the ".mp4" in "clip.mp4.mov"
		e := filepath.Ext(base)
		if e == "" || !detect.IsMediaExt(e) {
			break
		}
		b := base[:len(base)-len(e)]
		if b == "" {
			break
		}
		base = b
	}
	return base + ext
}

// doubledMediaExt catches extensions doubled by historical renames
// ("VID_...mp4.mp4", "clip.mp4.mov"): two media extensions in a row at the end.
func doubledMediaExt(path string) bool {
	e := filepath.Ext(path)
	if e == "" || !detect.IsMediaExt(e) {
		return false
	}
	return detect.IsMediaExt(filepath.Ext(path[:len(path)-len(e)]))
}

// isOurArtifact skips the tool's own database (and WAL/SHM sidecars) and the
// change-log sidecar that lives next to it.
func isOurArtifact(p, dbPath string) bool {
	base := filepath.Base(p)
	if base == "image_compressor.db" || base == "image_compressor.db-wal" || base == "image_compressor.db-shm" {
		return true
	}
	if p == changeLogPath(dbPath) {
		return true
	}
	if strings.HasSuffix(base, ".db") || strings.HasSuffix(base, ".db-wal") || strings.HasSuffix(base, ".db-shm") {
		// also skip the configured db and its sidecars if named differently
		for _, suf := range []string{"", "-wal", "-shm"} {
			if p == dbPath+suf {
				return true
			}
		}
	}
	return false
}

func fileExists(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}

func hashFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func logf(cfg *config.Config, format string, v ...any) {
	if cfg.Verbose {
		log.Printf(format, v...)
	} else {
		// keep a visible but compact line for real errors
		fmt.Fprintf(os.Stderr, "! "+format+"\n", v...)
	}
}

// r.logf logs an anomaly AND mirrors it into the change log so a double-click
// run keeps its full error trail on disk. In VT live-block mode the console
// write is MUTED: an error line would corrupt/scroll the fixed block. The err
// counter in the header and the journal (path printed at startup) carry every
// error instead. In single-line/pipe mode errors still print to stderr — there
// the is no live block to protect and stderr is the only channel.
func (r *Runner) logf(format string, v ...any) {
	if !r.multi {
		r.printBreak(func() { logf(r.cfg, format, v...) })
	}
	r.changes.record("error", "", "", 0, 0, 0, "", "", "", fmt.Sprintf(format, v...))
}

// printBreak safely emits a one-off console message (dry-run line, etc.) while
// the live block is up: erase the block, print the message on its own line,
// force the next repaint to start fresh below it. Without this, a raw write
// would corrupt the VT cursor bookkeeping and smear the block.
func (r *Runner) printBreak(print func()) {
	r.statusMu.Lock()
	if r.statusVis {
		n := r.blockLines()
		fmt.Printf("\x1b[%dA", n-1)
		for i := 0; i < n; i++ {
			fmt.Print("\x1b[2K")
			if i < n-1 {
				fmt.Print("\n")
			}
		}
		fmt.Print("\r")
		r.statusVis = false
		r.firstDraw.Store(true)
	}
	print()
	r.statusMu.Unlock()
}

// achievedFor renders the actual result quality stored in the DB ("achieved"
// column) — for future runs it answers "at what quality does this file already
// sit, is there room to squeeze further?". It leads with the encoder that won
// ("svt crf=34 vmaf=96.4", "avifenc q=85"): encoder choice is audit data like
// quality is, and the scales are not comparable across encoders.
func (r *Runner) achievedFor(k detect.Kind, v media.VideoResult, p media.PhotoResult) string {
	head := ""
	switch k {
	case detect.KindVideo:
		if v.CRF == 0 {
			return ""
		}
		if v.Codec != "" {
			head = v.Codec + " "
		}
		if v.VMAF >= 0 {
			return fmt.Sprintf("%scrf=%d vmaf=%.1f", head, v.CRF, v.VMAF)
		}
		return fmt.Sprintf("%scrf=%d", head, v.CRF)
	case detect.KindImage:
		if p.Q == 0 {
			return ""
		}
		if p.Codec != "" {
			head = p.Codec + " "
		}
		if p.VMAF >= 0 {
			return fmt.Sprintf("%sq=%d vmaf=%.1f", head, p.Q, p.VMAF)
		}
		return fmt.Sprintf("%sq=%d", head, p.Q)
	case detect.KindAnimated:
		if r.opts.PhotoLoss {
			return "lossless"
		}
		return fmt.Sprintf("q=%d", r.opts.PhotoQ)
	default:
		return ""
	}
}

// attemptsComment joins the encoder audit trail (every encode + VMAF pass with
// its duration) for the journal Comment column: "why did this file take N
// minutes" is then answerable from the log alone, no verbose rerun needed.
func attemptsComment(v media.VideoResult, p media.PhotoResult) string {
	if len(v.Attempts) > 0 {
		return strings.Join(v.Attempts, "; ")
	}
	if len(p.Attempts) > 0 {
		return strings.Join(p.Attempts, "; ")
	}
	return ""
}

// changeLogPath puts the audit log next to the DB, sharing its base name:
// <dir>/image_compressor.db -> <dir>/image_compressor.log
// (plain tab-separated text: Notepad-friendly, spreadsheets by accident).
func changeLogPath(dbPath string) string {
	dir := filepath.Dir(dbPath)
	base := strings.TrimSuffix(filepath.Base(dbPath), ".db")
	return filepath.Join(dir, base+".log")
}
