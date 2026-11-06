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

type Runner struct {
	cfg         *config.Config
	store       *store.Store
	loc         *tools.Locator
	opts        media.Opts
	videoOpts   media.Opts // video clone: fat thread slice + preset (see lanes)
	vsem        chan struct{}
	hsem        chan struct{} // big-file hash lanes: few concurrent cold reads
	vchain      media.VideoChain
	exif        string
	processRoot string
	hasAvifenc  bool
	hasFFmpeg   bool
	changes     *changeLog

	processed, skipDone, skipNotSmaller, skipUnknown, fixedExt, errored atomic.Int64
	origBytes, newBytes, scanBytes                                      atomic.Int64
	done                                                                atomic.Int64
	total                                                               int
	pruned                                                              int64 // journal rows GC'd at run end
	gcGuarded                                                           bool  // GC refused: walk scope < half the DB

	observedMu sync.Mutex          // guards observed (end-of-run journal GC)
	observed   map[string]struct{} // every content hash this walk laid eyes on

	// claims reserves honest output names for the duration of one swap.
	// Two source files can want the same name ("a.jpg" + "a.webp" copies,
	// "a.jpg" + "A.PNG" on a case-insensitive filesystem); without a claim a
	// rename would overwrite the other file's output and remove its source.
	claimMu sync.Mutex
	claims  map[string]bool

	// stemTwins counts how many media files the walk found under one honest
	// name stem in the same folder. Count >= 2 means two files want one output
	// name. Filled by gather, read-only while workers run.
	stemTwins map[string]int

	multi     bool        // VT fixed-height status block: counters + per-worker rows
	firstDraw atomic.Bool // first VT repaint of this run
	statusVis bool        // live block is currently on screen (guarded by statusMu)
	statusMu  sync.Mutex  // one repaint at a time (ticker goroutine vs workers)
	activeMu  sync.Mutex
	active    []activeJob // files the workers are chewing on right now
}

// activeJob is one file a worker is processing right now, for the live
// status line ("which files are being worked on this second, how long").
// The progress fields are video-only: the encode pass streams ffmpeg's
// out_time_ms back, so a 40-minute pass shows its real position instead of
// looking stuck. stage == "" means no encode pass is running (hash/skip/etc).
type activeJob struct {
	rel   string
	start time.Time
	stage string // ladder label ("crf28") or "queued" (waiting for a lane)
	curMs int64  // encoded position in the current pass
	durMs int64  // total media duration (header probe), 0 = unknown
}

func New(cfg *config.Config, st *store.Store, loc *tools.Locator) *Runner {
	opts := media.Opts{
		PhotoQ:       cfg.PhotoQ,
		PhotoLoss:    cfg.PhotoLoss,
		PhotoSpeed:   cfg.PhotoSpeed,
		PhotoVMAFMin: cfg.PhotoVMAF,
		VideoCRF:     cfg.VideoCRF,
		VideoVMAFMin: cfg.VideoVMAF,
		Encoder:      cfg.VideoEnc,
		VideoPreset:  cfg.VideoPreset,
		// Auto core split: every child encoder/vmaf gets its slice instead
		// of greedily grabbing all cores (N workers × all-cores children just
		// thrash each other). Thread count does NOT change encoder output
		// (aom/svt/libvmaf are thread-deterministic), so the nochange
		// stamps deliberately exclude it.
		CPUBudget: childBudget(cfg.Workers),
	}
	// Video lanes (the battle fix): SVT-AV1 given lp=2 on a 24-thread box
	// runs at a third of its speed and starves the whole machine (~55% CPU
	// in the FData run). Heavy video encodes therefore run a few at a time
	// with a fat thread slice each; photos (1-3 s avifenc jobs) keep the
	// original worker-wide split and never wait for a lane.
	lanes := videoLanes(cfg)
	videoOpts := opts
	videoOpts.CPUBudget = childBudget(lanes)
	return &Runner{
		cfg:        cfg,
		store:      st,
		loc:        loc,
		opts:       opts,
		videoOpts:  videoOpts,
		vsem:       make(chan struct{}, lanes),
		hsem:       make(chan struct{}, bigHashLanes),
		exif:       loc.Exiftool(),
		hasAvifenc: loc.Avifenc() != "",
		hasFFmpeg:  loc.FFmpeg() != "",
		observed:   map[string]struct{}{},
		claims:     map[string]bool{},
		stemTwins:  map[string]int{},
	}
}

// stemKey is the identity our naming rules give a file: its folder plus its
// name with the trailing (and chained) media extensions removed, case-folded
// where the filesystem folds case. Every file sharing a key gets the same
// finalPath ("x.jpg" and "x.mp4" both want "x.avif"), so a count above one
// means a name fight.
//
// This reads SUFFIXES on purpose: output names are a suffix game. It never
// decides what a file IS — classification and honesty stay magic-bytes-only
// (canon), exactly like videosLast which only queues by extension.
func stemKey(path string) string {
	stem := filepath.Base(path)
	for {
		e := filepath.Ext(stem)
		if e == "" || !detect.IsMediaExt(e) {
			break
		}
		t := stem[:len(stem)-len(e)]
		if t == "" {
			break
		}
		stem = t
	}
	key := stem
	if runtime.GOOS == "windows" {
		key = strings.ToLower(key)
	}
	return filepath.Join(filepath.Dir(path), key)
}

// countStem records one walked file under its name stem, keyed RELATIVE to the
// walked root so the same file seen through the --output mirror lands on the
// same key (the mirror keeps the relative layout). Only media-suffixed files
// are counted: they are the ones that can claim an output name (a same-named
// "x.txt" is not a rival). Gather time only.
func (r *Runner) countStem(path, walkRoot string) {
	if !detect.IsMediaExt(filepath.Ext(path)) {
		return
	}
	rel, err := filepath.Rel(walkRoot, path)
	if err != nil {
		rel = filepath.Base(path)
	}
	r.stemTwins[stemKey(rel)]++
}

// hasStemTwin reports whether another walked file in the same folder wants the
// same honest output name as this one (same stem under processRoot).
func (r *Runner) hasStemTwin(path string) bool {
	return r.stemTwins[stemKey(r.rel(path))] >= 2
}

// samePath compares paths the way the filesystem does them. On Windows
// "clip.MP4" and "clip.mp4" are ONE file, not two. Treating them as two is how
// an uppercase-extension clip lost its own fresh output: the swap renamed the
// file in place, then RemoveIfExists on the "old" name deleted the result.
func samePath(a, b string) bool {
	if a == b {
		return true
	}
	if runtime.GOOS == "windows" {
		return strings.EqualFold(a, b)
	}
	return false
}

// maxNameAttempts bounds the numbered-name search. Every attempt is one stat,
// and the loop stops at the first free name, so the bound is a runaway guard,
// not a practical limit: the live archive's longest name fight is TWO files,
// which asks for one numbered name. Reaching this ceiling means a folder holds
// that many same-stem media files; we stop and leave the file untouched rather
// than rename forever or overwrite anyone.
const maxNameAttempts = 1000

// claimTarget reserves a writable honest name for one swap and returns it.
//
// The requested honest name is preferred. When that name is already owned by a
// DIFFERENT file — two sources with one stem ("a.jpg" + "a.webp" copies,
// "a.jpg" + "A.PNG" on a case-insensitive filesystem, or a "hat" clip whose
// extracted frame wants the photo's name) — the rival's name is NOT taken
// over: a numbered name is reserved instead ("a (2).avif"). The reservation
// happens BEFORE encoding because safefs.AtomicReplace overwrites
// unconditionally: the FData archive lost 274 files (160 of them photos
// replaced by a frame pulled out of a same-named clip) once two sources were
// allowed to want one name.
//
// A same-name swap ("clip.mp4" -> "clip.mp4") replaces its own source and
// always passes. The caller must run release() when the swap is finished (or
// abandoned). chosen == "" means no name could be reserved.
func (r *Runner) claimTarget(src, target string) (chosen string, release func()) {
	noop := func() {}
	if samePath(src, target) {
		return target, noop
	}
	dir := filepath.Dir(target)
	base := filepath.Base(target)
	ext := filepath.Ext(base)
	stem := base[:len(base)-len(ext)]
	r.claimMu.Lock()
	defer r.claimMu.Unlock()
	for n := 1; n <= maxNameAttempts; n++ {
		cand := target
		if n > 1 {
			cand = filepath.Join(dir, fmt.Sprintf("%s (%d)%s", stem, n, ext))
		}
		key := cand
		if runtime.GOOS == "windows" {
			key = strings.ToLower(key)
		}
		if r.claims[key] || fileExists(cand) {
			continue
		}
		r.claims[key] = true
		return cand, func() {
			r.claimMu.Lock()
			delete(r.claims, key)
			r.claimMu.Unlock()
		}
	}
	return "", noop
}

// DefaultVideoLanes: how many heavy video encodes run side by side by auto.
// ~8 threads each is where SVT-AV1 finally unfolds (its lookahead/EBT
// pipeline stops scaling below ~6-8 threads); photos and skips still use
// every worker slot, they just queue behind lanes cheaply.
func DefaultVideoLanes() int {
	n := runtime.NumCPU() / 8
	if n < 1 {
		n = 1
	}
	return n
}

// videoLanes resolves the lane count: explicit --video-workers, or auto;
// never more lanes than workers (a lane needs a worker to drive it) and
// never less than one.
func videoLanes(cfg *config.Config) int {
	lanes := cfg.VideoWorkers
	if lanes <= 0 {
		lanes = DefaultVideoLanes()
	}
	w := cfg.Workers
	if w < 1 {
		w = 1
	}
	if lanes > w {
		lanes = w
	}
	return lanes
}

// acquireLane takes one heavy-encode lane (video AND animated: multi-frame
// AV1 jobs starve on a thin thread slice, the battle proved it) and shows
// "queued" in the slot while waiting. false = stop requested.
func (r *Runner) acquireLane(ctx context.Context, rel string) bool {
	if r.multi {
		r.trackProgress(rel, "queued", 0)
	}
	select {
	case r.vsem <- struct{}{}:
		return true
	case <-ctx.Done():
		return false
	}
}

const (
	// bigHashThreshold: files at least this large are hashed through the
	// big-file lanes. Below it (photos, short clips) a parallel read or two
	// does not hurt a spindle.
	bigHashThreshold = 64 << 20
	// bigHashLanes: how many big files are read/hashed at once. The archive
	// HDD measured ~22 MB/s aggregate with 12 concurrent readers (seek
	// thrash, ~30 KB per read before the buffer fix); a few sequential
	// streams let the head stream instead. Small files keep the full worker
	// parallelism, and cache hits never take a lane.
	bigHashLanes = 3
)

// acquireHashLane gates cold hashing of big files to a few concurrent reads.
// The returned release MUST be called right after hashing — never held
// through the encode (hashing and encoding are different bottlenecks). While
// waiting the slot says "hash queued"; a stop request returns ok=false and
// the file is left untouched for the next run.
func (r *Runner) acquireHashLane(ctx context.Context, rel string, size int64) (release func(), ok bool) {
	noop := func() {}
	if size < bigHashThreshold {
		return noop, true
	}
	r.trackStage(rel, "hash queued")
	select {
	case r.hsem <- struct{}{}:
		return func() { <-r.hsem }, true
	case <-ctx.Done():
		return noop, false
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

// videoSchedExts are file extensions treated as heavy encodes for QUEUEING
// only (videosLast). This never decides what a file IS: classification and
// honest extensions remain magic-bytes-only (canon).
var videoSchedExts = map[string]bool{
	".mp4": true, ".mov": true, ".mkv": true, ".webm": true, ".avi": true, ".m4v": true,
	".gif": true, // animated: a multi-frame AV1 encode, queued with the heavy jobs
}

// videosLast stably moves extension-video files to the back of the queue:
// photos, animated gifs and non-media copies drain through every worker,
// then videos spread across the lanes. Mislabelled files keep their
// magic-byte behavior either way; only the order they are attempted in
// changes.
func videosLast(files []string) []string {
	head := make([]string, 0, len(files))
	tail := make([]string, 0)
	for _, f := range files {
		if videoSchedExts[strings.ToLower(filepath.Ext(f))] {
			tail = append(tail, f)
		} else {
			head = append(head, f)
		}
	}
	return append(head, tail...)
}

// stampFor fingerprints the settings that decide "can this shrink?". A nochange
// record with a matching stamp is skipped on later runs; a different stamp makes
// us re-evaluate (so changing --photo-q/--photo-vmaf-min/--video-crf/encoder/
// --video-preset takes effect).
func (r *Runner) stampFor(k detect.Kind) string {
	switch k {
	case detect.KindVideo:
		return fmt.Sprintf("vid|%d|%.0f|%s|preset=%d|ff=%v|still=%v", r.opts.VideoCRF, r.opts.VideoVMAFMin, r.opts.Encoder, r.opts.VideoPreset, r.hasFFmpeg, r.cfg.Still)
	case detect.KindAnimated:
		return fmt.Sprintf("anim|%d|%v|speed=%d|avifenc=%v|ff=%v", r.opts.PhotoQ, r.opts.PhotoLoss, r.opts.PhotoSpeed, r.hasAvifenc, r.hasFFmpeg)
	default:
		return fmt.Sprintf("img|%d|%.0f|%v|speed=%d|avifenc=%v|ff=%v", r.opts.PhotoQ, r.opts.PhotoVMAFMin, r.opts.PhotoLoss, r.opts.PhotoSpeed, r.hasAvifenc, r.hasFFmpeg)
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
				if fileExists(s.FinalPath) {
					// The honest name is occupied by something that is NOT our
					// output (its hash did not match above): never swallow it.
					// The staged swap is dropped; the source is still on disk
					// (the rename never happened) and gets reprocessed.
					safefs.RemoveIfExists(s.TmpPath)
					_ = r.store.DropStaged(ctx, s.ID)
					log.Printf("recover %s: target taken by another file, swap dropped", s.FinalPath)
					continue
				}
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

	// Probe AV1 encoders once and resolve the GPU->CPU fallback chain.
	avail := media.ProbeAV1Encoders(ctx, r.loc)
	r.vchain = r.opts.BuildVideoChain(avail)
	fmt.Printf("Video encoders, mode %q: %s\n", r.opts.Encoder, chainString(r.vchain))
	fmt.Printf("Heavy encodes (video/animated): %d lanes side by side, %d threads each\n", cap(r.vsem), r.videoOpts.CPUBudget)
	fmt.Printf("Big-file hashing: %d at a time (>= %d MB), small files unrestricted\n", cap(r.hsem), bigHashThreshold>>20)
	if r.cfg.Still {
		fmt.Println("Still-image video collapse: on, one static frame with a soundtrack becomes a photo, use --no-still to keep such files as videos")
	} else {
		fmt.Println("Still-image video collapse: off, hat-shaped videos stay normal videos")
	}

	files, err := r.gather(ctx)
	if err != nil {
		return err
	}
	// Scheduling hint only (detection still trusts nothing but magic bytes):
	// videos queue last so quick photo/skip work drains the workers first,
	// instead of 12 workers parking on 3 hot lanes while photos wait 40 min.
	files = videosLast(files)
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
			slot := r.trackStart(f)
			r.process(gctx, f)
			r.trackEnd(slot)
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
		// The end-of-run summary prints BELOW the block: the final worker
		// rows stay readable (the battle complaint: "the done-line wiped all
		// the previous rows"). A single newline moves below the block whose
		// cursor already sits on its last row.
		fmt.Print("\n")
	} else if tty && !r.multi {
		fmt.Print("\r" + strings.Repeat(" ", 110) + "\r")
	}
	if err != nil {
		return err
	}
	// Journal GC (the zombie sweep): a completed walk observed every hash that
	// still exists in the tree, so rows nobody observed are records of content
	// that is gone (deleted by hand, replaced from outside, an archive redo
	// restoring originals). Deleting them keeps the DB a truthful mirror and
	// makes manual pre-redo cleanup unnecessary. Never for dry-run/fix-ext
	// (those walks don't hash media), never for --output runs, never after a
	// stop — and PruneUnseen itself refuses if the walk saw < half the DB.
	if !r.cfg.DryRun && !r.cfg.FixExt && r.cfg.InPlace() && ctx.Err() == nil {
		n, guarded, gerr := r.store.PruneUnseen(context.WithoutCancel(ctx), r.observed)
		switch {
		case gerr != nil:
			r.logf("journal gc: %v", gerr)
		default:
			r.pruned, r.gcGuarded = n, guarded
		}
	}
	// The same summary the console shows, as one journal line: if the console
	// window gets closed (double-click UX), the run's verdict is still on disk.
	r.changes.summary(r.Summary())
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
	// ETA deliberately absent: the byte-rate estimate lied under the
	// videos-queued-last order (photo phase runs fast MB/s, then the video
	// giants rewrite it upward several times). Counters + saved tell the truth.
	header := fmt.Sprintf("[%s] %d/%d (%d%%)  ok:%d already:%d nofit:%d ext:%d err:%d  |  saved %.0f MB",
		fmtDur(el), done, r.total, pct,
		r.processed.Load(), r.skipDone.Load(), r.skipNotSmaller.Load(),
		r.fixedExt.Load(), r.errored.Load(),
		savedMB)
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

// statusBlock builds the fixed-height live block: counter header + a column
// title row + exactly r.cfg.Workers slot rows (slot i is always worker i;
// empty slots show dashes so nothing ever shifts vertically). maxw clips a
// row to the console width (rune-safe) so it never wraps.
func (r *Runner) statusBlock(header string, maxw int) []string {
	workers := r.cfg.Workers
	if workers < 1 {
		workers = 1
	}
	r.activeMu.Lock()
	jobs := make([]activeJob, len(r.active))
	copy(jobs, r.active)
	r.activeMu.Unlock()

	lines := make([]string, 0, workers+2)
	// clip budget leaves room for clip's ellipsis so the row (with "...")
	// still fits the console width and never wraps.
	clipw := maxw - 3
	if clipw < 8 {
		clipw = 8
	}
	lines = append(lines, header)
	// column titles: same fixed widths as the slot rows below
	lines = append(lines, clip(fmt.Sprintf("%2s  %8s  %-21s  %s", "#", "elapsed", "stage/position", "file"), clipw))
	now := time.Now()
	for i := 0; i < workers; i++ {
		if i < len(jobs) && jobs[i].rel != "" {
			j := jobs[i]
			lines = append(lines, clip(fmt.Sprintf("%2d  %8s  %-21s  %s", i+1, fmtDurShort(now.Sub(j.start)), slotPos(j), j.rel), clipw))
		} else {
			lines = append(lines, fmt.Sprintf("%2d  %8s", i+1, "--"))
		}
	}
	return lines
}

// slotPos renders the middle column of a worker slot: the live position of
// the video encode pass ("crf28 4:31/13:22"), the queue wait for a lane
// ("queued"), or empty for everything else (photos, hashing, metadata).
// The battle complaint it answers: "is this file stuck or moving?".
func slotPos(j activeJob) string {
	if j.stage == "" {
		return ""
	}
	if j.stage == "queued" {
		return "queued"
	}
	if j.curMs < 0 {
		return j.stage // phase marker without a timeline (e.g. "crf34 vmaf":
	} // encode finished, the metric pass runs, position is frozen by design
	if j.curMs <= 0 && j.durMs <= 0 {
		return j.stage // photo/animated ladder step (q85, lossless): no timeline
	}
	cur := fmtDurShort(time.Duration(j.curMs) * time.Millisecond)
	if j.durMs <= 0 {
		return j.stage + " " + cur // duration unknown: position alone still proves movement
	}
	return j.stage + " " + cur + "/" + fmtDurShort(time.Duration(j.durMs)*time.Millisecond)
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
	return w + 2 // counter header + column titles + one row per worker slot
}

// trackStart/trackEnd maintain the live fixed-slot status rows (only when the
// status block is on; skips are tracked too, they are cheap). Slots are
// positional: slot i is ALWAYS worker-row i — when a file finishes, its row
// falls back to dashes and the next file re-uses that row. No row ever shifts
// (the battle complaint: "the runner jumps between rows, cells move up").
// trackStart returns the claimed slot index (-1 when not tracking).
func (r *Runner) trackStart(path string) int {
	if !r.multi {
		return -1
	}
	w := r.cfg.Workers
	if w < 1 {
		w = 1
	}
	r.activeMu.Lock()
	if len(r.active) < w {
		grow := make([]activeJob, w)
		copy(grow, r.active)
		r.active = grow
	}
	idx := -1
	for i := range r.active {
		if r.active[i].rel == "" {
			idx = i
			break
		}
	}
	if idx >= 0 {
		r.active[idx] = activeJob{rel: r.rel(path), start: time.Now()}
	}
	r.activeMu.Unlock()
	return idx
}

func (r *Runner) trackEnd(idx int) {
	if idx < 0 {
		return
	}
	r.activeMu.Lock()
	if idx < len(r.active) {
		r.active[idx] = activeJob{}
	}
	r.activeMu.Unlock()
}

// trackTotal records the media duration (ms) of an in-flight file once the
// header probe succeeded, so the slot can show position/total. Takes rel
// directly: the encode site already resolved it.
func (r *Runner) trackTotal(rel string, durMs int64) {
	if !r.multi {
		return
	}
	r.activeMu.Lock()
	for i := range r.active {
		if r.active[i].rel == rel {
			r.active[i].durMs = durMs
			break
		}
	}
	r.activeMu.Unlock()
}

// trackStage labels a non-encoder phase (hash, still?, meta) so the slot
// column never sits blank while the runner is actually working. Those phases
// can take minutes on a big video (hashing a 300 MB file behind 12 readers on
// a spinning disk); a blank cell reads as "stuck", a label reads as "busy".
func (r *Runner) trackStage(rel, label string) {
	r.trackProgress(rel, label, 0)
}

// rememberSeen records the path -> hash cache after a stable decision (skip,
// nochange, or swap) so the next full run does not read the file again. The
// key is the ABSOLUTE path: a shared --db used over different roots must never
// collide two files that happen to share a relative name. Never in dry-run:
// that mode must not write anything, and never without a real stat (size < 0).
func (r *Runner) rememberSeen(ctx context.Context, path string, size, mtime int64, hash string) {
	if r.cfg.DryRun || size < 0 || hash == "" {
		return
	}
	if err := r.store.RememberPath(ctx, path, size, mtime, hash); err != nil {
		r.logf("seen %s: %v", path, err)
	}
}

// trackProgress applies one ffmpeg -progress sample (stage label + stream
// position) to the in-flight slot. Called from the encoder's output scanner;
// cheap and lock-scoped, progress lines arrive a few times per second.
func (r *Runner) trackProgress(rel, stage string, curMs int64) {
	if !r.multi {
		return
	}
	r.activeMu.Lock()
	for i := range r.active {
		if r.active[i].rel == rel {
			r.active[i].stage = stage
			r.active[i].curMs = curMs
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
		r.countStem(p, src) // name-jam map: who else wants this output name
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
		r.fixExtOnly(ctx, path, res)
		return
	}

	rel := r.rel(path)

	// Hash-cache fast path (store.seen): if this exact path still carries the
	// size+mtime we hashed last time, the content cannot have changed (archive
	// media is never edited in place; an edit/restore bumps mtime). A hit
	// skips reading gigabytes of video; a miss falls through to the real hash.
	// The key is the absolute path (see rememberSeen).
	var h string
	size, mtime := int64(-1), int64(0)
	if fi, serr := os.Stat(path); serr == nil {
		size, mtime = fi.Size(), fi.ModTime().UnixNano()
		if cached, ok, cerr := r.store.SeenHash(ctx, path, size, mtime); cerr == nil && ok {
			h = cached
		}
	}
	if h == "" {
		release, ok := r.acquireHashLane(ctx, rel, size)
		if !ok {
			return // stop requested while queued for a read lane; nothing touched
		}
		r.trackStage(rel, "hash")
		h, err = hashFile(path)
		release()
		if err != nil {
			r.errored.Add(1)
			r.logf("hash %s: %v", path, err)
			return
		}
	}
	// Journal GC witness: this content exists in the tree right now.
	r.observe(h)

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
				p := r.ensureHonestExt(ctx, path, res) // lying ext is fixed even for untouched files
				r.rememberSeen(ctx, p, size, mtime, h)
				return
			}
		default: // compressed: our (or an adopted old) canonical output on disk
			r.skipDone.Add(1)
			r.addSize(path, path)
			p := r.ensureHonestExt(ctx, path, res)
			r.rememberSeen(ctx, p, size, mtime, h)
			return
		}
	}

	// "Hat" detection: a social-media video that is really one static frame
	// with a soundtrack glued on collapses into a photo (see media.
	// StillVideo). Runs after idempotency checks so processed/skipped files
	// never pay for probing; both witnesses must agree before we collapse.
	//
	// A same-stem sibling blocks the collapse: if the folder already holds
	// "x.jpg" (or "x.png"/"x.webp") next to "x.mp4", that photo IS the picture
	// the clip would contribute, and the frame would want the photo's own
	// honest name. Collapsing there duplicated the picture and — before names
	// were claimed — overwrote the compressed photo: 160 of the 274 files the
	// live archive lost died exactly this way. Such clips now run the normal
	// video ladder (or stay as they are if AV1 cannot beat them).
	still := false
	if res.Kind == detect.KindVideo && r.cfg.Still && r.hasFFmpeg && !r.hasStemTwin(path) {
		// The pixel verification decodes the whole file: minutes on a long
		// video, so the slot must say what is happening.
		r.trackStage(rel, "still?")
		still = media.StillVideo(ctx, r.loc, path)
	}

	// If a journal row existed here, it was "nochange" with a changed settings
	// stamp (the compressed outcome returns above): re-evaluating and failing
	// to shrink again CONFIRMS the old decision, so it counts in `already`,
	// not `nofit` (the user's counter semantics: nofit = new verdicts only).
	retried := processed

	// Dry run: just report what would change.
	if r.cfg.DryRun {
		target := r.finalPath(path, res)
		if still {
			target = replaceExt(target, detect.FmtAVIF.HonestExt())
		}
		note := ""
		if still {
			note = ", collapse still-image video to photo"
		}
		if !samePath(target, path) && fileExists(target) {
			note += ", honest name owned by another file -> numbered name instead"
		}
		r.printBreak(func() { fmt.Printf("[dry-run] %s -> %s%s\n", rel, filepath.Base(target), note) })
		return
	}

	r.encodeSwap(ctx, path, rel, h, res, still, retried)
}

// observe records a content hash this walk has seen (end-of-run journal GC).
func (r *Runner) observe(h string) {
	r.observedMu.Lock()
	r.observed[h] = struct{}{}
	r.observedMu.Unlock()
}

func (r *Runner) encodeSwap(ctx context.Context, path, rel, srcHash string, res detect.Result, still, retried bool) {
	t0 := time.Now() // per-file wall time -> journal Seconds column (how long this file took)
	// The settings stamp must be the one the idempotency check will recompute,
	// and that check uses the kind DETECTED from the bytes (process compares
	// rec.Stamp with stampFor(res.Kind)). A "hat" collapse rewrites res.Kind
	// below, so a refused collapse stamped with the rewritten kind could never
	// match again and the file was re-encoded on EVERY run (battle:
	// azart_zay\azart_zay-20230407-0001.mp4 in every journal section).
	origKind := res.Kind
	if still {
		// The file collapses video -> photo: everything downstream (target
		// extension, DB kind/format, achieved string, journal) follows the
		// image branch. The original file on disk is still the MP4 at this
		// point; media.CollapseStill extracts the frame itself.
		res.Kind = detect.KindImage
		res.Format = detect.FmtAVIF
	}
	finalPath := r.finalPath(path, res) // honest extension target
	// Reserve a name BEFORE encoding: one name must never be wanted by two
	// different files. If the honest one is already owned by someone else this
	// file gets a numbered name ("x (2).avif") instead of overwriting it, so
	// both sources survive and both still get compressed.
	chosen, releaseTarget := r.claimTarget(path, finalPath)
	if chosen == "" {
		releaseTarget()
		r.errored.Add(1)
		r.logf("name %s: no free honest name available near %s, file left untouched", rel, r.rel(finalPath))
		return
	}
	defer releaseTarget()
	nameTakenNote := ""
	if !samePath(chosen, finalPath) {
		nameTakenNote = "name taken: " + filepath.Base(finalPath) + " owned by another file; "
		r.noticef("name taken %s: %s is owned by another file, writing %s", rel, r.rel(finalPath), r.rel(chosen))
	}
	finalPath = chosen
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
	// Photo/animated ladder step labels feed the same status slot column the
	// video position lives in: the display is consistent across kinds, only
	// the content differs (a still frame has no timeline to show position on).
	var pprog func(label string)
	if r.multi {
		pprog = func(label string) { r.trackProgress(rel, label, 0) }
	}
	switch res.Kind {
	case detect.KindImage:
		if still {
			pinfo, encErr = media.CollapseStill(ctx, r.loc, r.opts, path, tmp, origSize, pprog)
		} else {
			pinfo, encErr = media.EncodeImage(ctx, r.loc, r.opts, path, tmp, origSize, pprog)
		}
	case detect.KindAnimated:
		// Animated GIF/WebP is a real multi-frame AV1 encode: a big gif can
		// burn minutes on a thin -j2 slice, exactly the video starvation.
		// So it runs on a heavy lane with the fat budget, shows "queued"
		// while waiting, and records which encoder actually won.
		if r.acquireLane(ctx, rel) {
			pinfo.Codec, encErr = media.EncodeAnimated(ctx, r.loc, r.videoOpts, path, tmp, pprog)
			<-r.vsem
		} else {
			encErr = ctx.Err() // stop request while queued: not an error (checked below)
		}
	case detect.KindVideo:
		// Heavy AV1 passes run a few files at a time on the video lanes
		// (each lane child gets cores/lanes threads, so SVT-AV1 unfolds);
		// the wait for a lane is visible in the slot as "queued". The
		// duration header probe (no decode, no ffprobe per canon) lets the
		// slot show position/total for the running pass.
		if r.multi {
			if dur := media.InspectDuration(ctx, r.loc, path); dur >= 0 {
				r.trackTotal(rel, dur)
			}
		}
		if r.acquireLane(ctx, rel) {
			var prog func(label string, curMs int64)
			if r.multi {
				prog = func(label string, curMs int64) { r.trackProgress(rel, label, curMs) }
			}
			vinfo, encErr = media.EncodeVideo(ctx, r.loc, r.videoOpts, r.vchain, path, tmp, origSize, prog)
			<-r.vsem
		} else {
			encErr = ctx.Err() // stop request while queued: not an error (checked below)
		}
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
		if retried {
			r.skipDone.Add(1) // re-confirming the journal's "nochange" under changed settings
		} else {
			r.skipNotSmaller.Add(1) // new "could not shrink" verdict of this run
		}
		r.addSize(path, path)
		_ = r.store.MarkNoChange(ctx, srcHash, rel, origKind.String(), r.stampFor(origKind))
		r.changes.record("not smaller", rel, rel, origSize, origSize, time.Since(t0).Seconds(), res.Kind.String(), mediaFormatFor(res, finalPath), "", hatNote(encNote, still))
		p := r.ensureHonestExt(ctx, path, res)
		if oi, serr := os.Stat(p); serr == nil {
			r.rememberSeen(ctx, p, oi.Size(), oi.ModTime().UnixNano(), srcHash)
		}
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
	r.trackStage(rel, "meta")
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
		if retried {
			r.skipDone.Add(1) // re-confirming the journal's "nochange" under changed settings
		} else {
			r.skipNotSmaller.Add(1) // new "could not shrink" verdict of this run
		}
		r.addSize(path, path)
		_ = r.store.MarkNoChange(ctx, srcHash, rel, origKind.String(), r.stampFor(origKind))
		r.changes.record("not smaller", rel, rel, origInfo.Size(), newInfo.Size(), time.Since(t0).Seconds(), res.Kind.String(), mediaFormatFor(res, finalPath), "", hatNote(encNote, still))
		p := r.ensureHonestExt(ctx, path, res)
		if oi, serr := os.Stat(p); serr == nil {
			r.rememberSeen(ctx, p, oi.Size(), oi.ModTime().UnixNano(), srcHash)
		}
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
	// The swapped-in content now exists on disk: the walk already passed this
	// file, so only an explicit observation keeps its row alive at GC time.
	r.observe(newHash)
	// Commit must land even if a stop request arrives right here: the rename
	// already happened, so the DB record has to follow it (WithoutCancel).
	if err := r.store.CommitSwap(context.WithoutCancel(ctx), sid, newHash, newRel, res.Kind.String(), achieved); err != nil {
		r.errored.Add(1)
		r.logf("commit %s: %v", rel, err)
		return
	}

	// Restore NTFS creation time onto the swapped-in file (Windows; no-op elsewhere).
	// Uses the pre-swap capture so same-name swaps (mp4->mp4) stay honest too.
	if err := meta.SetBirthTime(finalPath, srcBirth); err != nil {
		r.logf("birthtime %s: %v", newRel, err)
	}
	// The new content is stable now: cache its triple so the next run skips the
	// read. The rename carries an old key when the extension changed. On
	// Windows a case-only extension change ("clip.MP4" -> "clip.mp4") is the
	// SAME file, not a rename.
	if !samePath(path, finalPath) {
		if err := r.store.RenamePath(ctx, path, finalPath); err != nil {
			r.logf("seen rename %s: %v", newRel, err)
		}
	}
	if fi, serr := os.Stat(finalPath); serr == nil {
		r.rememberSeen(ctx, finalPath, fi.Size(), fi.ModTime().UnixNano(), newHash)
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
	r.changes.record(action, rel, newRel, srcSize, dstSize, time.Since(t0).Seconds(), res.Kind.String(), mediaFormatFor(res, finalPath), achieved, nameTakenNote+hatNote(encNote, still))
	if !samePath(finalPath, path) {
		safefs.RemoveIfExists(path)
	}
}

// ensureHonestExt renames a media file whose extension lies, EVEN when the
// file is not compressed (user rule: lying extensions are always fixed —
// viewers must never be handed a file whose bytes contradict its name).
// Doubled/chained media extensions ("VID_x.mp4.mp4", "clip.mp4.mov" — noise
// left by earlier tools' renames) are cleaned up the same way.
// If the honest name is owned by a DIFFERENT file, this file takes a numbered
// honest name ("x (2).webp") — honesty is fixed either way and no file is
// swallowed. DB decisions are hash-based, so no stored-filename rewrite is
// required. Call AFTER addSize (a rename would make the old path unstatable).
// Returns the current path: the same one when nothing was renamed, the honest
// one otherwise (the caller uses it to keep the seen cache in sync).
func (r *Runner) ensureHonestExt(ctx context.Context, path string, res detect.Result) string {
	if !res.ExtensionLies && !doubledMediaExt(path) {
		return path
	}
	honest := res.Format.HonestExt()
	if honest == "" {
		return path
	}
	newPath := replaceExt(path, honest)
	if newPath == path {
		return path
	}
	if r.cfg.DryRun {
		r.printBreak(func() { fmt.Printf("[dry-run] fix-ext: %s -> %s\n", r.rel(path), r.rel(newPath)) })
		return path
	}
	// Reserve the honest name first. If another file owns it, this rename gets a
	// numbered honest name ("x (2).webp") instead of swallowing that file: the
	// lying extension is still fixed (canon: honesty always), nothing is lost.
	chosen, release := r.claimTarget(path, newPath)
	if chosen == "" {
		release()
		r.logf("fix-ext %s: no free honest name available near %s, keeping name", r.rel(path), r.rel(newPath))
		return path
	}
	defer release()
	newPath = chosen
	var sz int64
	if fi, err := os.Stat(path); err == nil {
		sz = fi.Size()
	}
	if err := os.Rename(path, newPath); err != nil {
		r.errored.Add(1)
		r.logf("fix-ext %s: %v", r.rel(path), err)
		return path
	}
	// The re-read cache follows the rename; otherwise the old key would linger
	// forever and the new name would be hashed once for nothing.
	if err := r.store.RenamePath(ctx, path, newPath); err != nil {
		r.logf("seen rename %s: %v", r.rel(newPath), err)
	}
	r.fixedExt.Add(1)
	r.changes.record("ext renamed", r.rel(path), r.rel(newPath), sz, sz, 0, res.Kind.String(), honest, "", "")
	return newPath
}

func (r *Runner) fixExtOnly(ctx context.Context, path string, res detect.Result) {
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
	// Reserve the honest name first: --fix-ext must never rename one file on top
	// of another that already owns it. A numbered honest name is used instead, so
	// the lie is still fixed and both files survive.
	chosen, release := r.claimTarget(path, newPath)
	if chosen == "" {
		release()
		r.logf("fix-ext %s: no free honest name available near %s, keeping name", r.rel(path), r.rel(newPath))
		return
	}
	defer release()
	newPath = chosen
	if err := os.Rename(path, newPath); err != nil {
		r.errored.Add(1)
		r.logf("fix-ext %s: %v", r.rel(path), err)
		return
	}
	// update stored filename mapping for the known hash (a mass --fix-ext run
	// over the archive still reads every file: big ones go through the read
	// lanes so they do not thrash the disk)
	var size int64 = -1
	if fi, serr := os.Stat(newPath); serr == nil {
		size = fi.Size()
	}
	release, ok := r.acquireHashLane(ctx, r.rel(newPath), size)
	if ok {
		h, herr := hashFile(newPath)
		release()
		if herr == nil {
			_ = r.store.MarkCompressed(context.Background(), h, r.rel(newPath), res.Kind.String(), "")
			_ = r.store.RenamePath(context.Background(), path, newPath)
			if fi, serr := os.Stat(newPath); serr == nil {
				_ = r.store.RememberPath(context.Background(), newPath, fi.Size(), fi.ModTime().UnixNano(), h)
			}
		}
	}
	r.processed.Add(1)
	r.addSize(path, newPath)
}

func (r *Runner) finalize(ctx context.Context, sid int64, finalPath, expectedHash string) error {
	res, err := detect.Detect(finalPath)
	if err != nil {
		return r.store.DropStaged(ctx, sid)
	}
	return r.store.CommitSwap(ctx, sid, expectedHash, r.rel(finalPath), res.Kind.String(), "")
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
	out := fmt.Sprintf(
		"Done. Compressed: %d, already processed: %d, not smaller: %d, extensions fixed: %d, non-media: %d, errors: %d\n"+
			"Size: %.2f MB -> %.2f MB = %.2f%% saved",
		r.processed.Load(), r.skipDone.Load(), r.skipNotSmaller.Load(), r.fixedExt.Load(), r.skipUnknown.Load(), r.errored.Load(),
		ob, nb, pct,
	)
	switch {
	case r.gcGuarded:
		// GC refused: this walk saw less than half of what the DB knows
		// (subfolder run against a shared --db). Nothing was deleted; say so.
		out += "\nJournal pruned: 0 (skipped: the walk saw under half of the DB — running over a subfolder with a shared --db?)"
	case r.pruned > 0:
		out += fmt.Sprintf("\nJournal pruned: %d stale entries", r.pruned)
	}
	return out
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
	// 1 MiB reads instead of io.Copy's 32 KiB default: on the archive HDD the
	// small buffer produced ~750 tiny reads/s per worker and 22 MB/s aggregate
	// across 12 hashers (measured on the live run); large sequential reads
	// let the drive stream instead of seek-thrash.
	buf := make([]byte, 1<<20)
	if _, err := io.CopyBuffer(h, f, buf); err != nil {
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

// r.noticef emits an INFORMATIONAL line (a numbered name was taken, etc.):
// console only, never the journal. Only real anomalies belong in the journal's
// error column — a swap that succeeded under a numbered name already has its
// own row ("compressed" with the "name taken:" Comment prefix), so mirroring
// the notice would fill the log with fake errors (~100 of them over the live
// archive). printBreak keeps the live block intact in VT mode.
func (r *Runner) noticef(format string, v ...any) {
	r.printBreak(func() { fmt.Printf(format+"\n", v...) })
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
// and how it was driven ("svt p8 crf=34 vmaf=96.4", "avifenc s6 q=85"):
// encoder choice is audit data like quality is, and the scales are not
// comparable across encoders. The SVT preset and the avifenc speed ride along
// (both change size/quality, so "pushed at 8 or at 6 / at s6 or s4" decides
// whether re-running is worth it; speed only appears for the tool that uses
// it). Lossless outputs say "lossless" — the q number there would lie.
// 10-bit is canon-on for every AV1 output — invariant, not recorded.
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
		if v.Codec == "svt" && r.opts.VideoPreset > 0 {
			head += fmt.Sprintf("p%d ", r.opts.VideoPreset)
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
		if r.opts.PhotoLoss {
			return head + "lossless" // q-scale is meaningless there
		}
		if r.opts.PhotoSpeed > 0 {
			head += fmt.Sprintf("s%d ", r.opts.PhotoSpeed) // avifenc speed: the photo twin of the SVT preset marker
		}
		if p.VMAF >= 0 {
			return fmt.Sprintf("%sq=%d vmaf=%.1f", head, p.Q, p.VMAF)
		}
		return fmt.Sprintf("%sq=%d", head, p.Q)
	case detect.KindAnimated:
		if p.Codec != "" {
			head = p.Codec + " "
		}
		if r.opts.PhotoLoss {
			return head + "lossless"
		}
		if r.opts.PhotoSpeed > 0 {
			head += fmt.Sprintf("s%d ", r.opts.PhotoSpeed)
		}
		return fmt.Sprintf("%sq=%d", head, r.opts.PhotoQ)
	default:
		return ""
	}
}

// hatNote marks a "not smaller" verdict that came from a video hat: the Kind
// column says "image" (the collapse rewrites it), which would otherwise read as
// if the file had always been a photo.
func hatNote(note string, still bool) string {
	if !still {
		return note
	}
	if note == "" {
		return "hat: one kept frame not smaller than the clip"
	}
	return "hat: " + note
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
