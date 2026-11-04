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
	"strings"
	"sync/atomic"

	"github.com/Llloooggg/ImageCompressor/internal/config"
	"github.com/Llloooggg/ImageCompressor/internal/detect"
	"github.com/Llloooggg/ImageCompressor/internal/media"
	"github.com/Llloooggg/ImageCompressor/internal/meta"
	"github.com/Llloooggg/ImageCompressor/internal/safefs"
	"github.com/Llloooggg/ImageCompressor/internal/store"
	"github.com/Llloooggg/ImageCompressor/internal/tools"

	"golang.org/x/sync/errgroup"
)

const toolVersion = "0.1.0"
const toolSelf = "mediacompressor"

type Runner struct {
	cfg     *config.Config
	store   *store.Store
	loc     *tools.Locator
	opts    media.Opts
	vchain  media.VideoChain
	exif    string
	legacy  map[string]struct{}
	processRoot string
	hasAvifenc  bool
	hasFFmpeg   bool

	processed, skipDone, skipNotSmaller, skipUnknown, errored atomic.Int64
	origBytes, newBytes                                        atomic.Int64
	done                                                       atomic.Int64
	total                                                      int
}

func New(cfg *config.Config, st *store.Store, loc *tools.Locator) *Runner {
	return &Runner{
		cfg:        cfg,
		store:      st,
		loc:        loc,
		opts:       media.Opts{PhotoQ: cfg.PhotoQ, PhotoLoss: cfg.PhotoLoss, VideoCRF: cfg.VideoCRF, Encoder: cfg.VideoEnc},
		exif:       loc.Exiftool(),
		hasAvifenc: loc.Avifenc() != "",
		hasFFmpeg:  loc.FFmpeg() != "",
	}
}

// stampFor fingerprints the settings that decide "can this shrink?". A nochange
// record with a matching stamp is skipped on later runs; a different stamp makes
// us re-evaluate (so changing --photo-q/--video-crf/encoder takes effect).
func (r *Runner) stampFor(k detect.Kind) string {
	switch k {
	case detect.KindVideo:
		return fmt.Sprintf("vid|%d|%s|ff=%v", r.opts.VideoCRF, r.opts.Encoder, r.hasFFmpeg)
	case detect.KindAnimated:
		return fmt.Sprintf("anim|%d|%v|avifenc=%v|ff=%v", r.opts.PhotoQ, r.opts.PhotoLoss, r.hasAvifenc, r.hasFFmpeg)
	default:
		return fmt.Sprintf("img|%d|%v|avifenc=%v|ff=%v", r.opts.PhotoQ, r.opts.PhotoLoss, r.hasAvifenc, r.hasFFmpeg)
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
		log.Printf("cleanup temps: %v", err)
	}

	if r.cfg.Migrate {
		lu, err := r.store.LegacyUnmigratedHashes(ctx)
		if err != nil {
			return fmt.Errorf("legacy set: %w", err)
		}
		r.legacy = lu
	}

	// Probe AV1 encoders once and resolve the GPU->CPU fallback chain.
	avail := media.AvailableEncoders(ctx, r.loc)
	r.vchain = r.opts.BuildVideoChain(avail)
	fmt.Printf("Видео-кодировщик (режим %q): %s\n", r.opts.Encoder, chainString(r.vchain))

	files, err := r.gather(ctx)
	if err != nil {
		return err
	}
	r.total = len(files)
	fmt.Printf("Найдено файлов: %d\n", r.total)

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(r.cfg.Workers)
	for _, f := range files {
		f := f
		g.Go(func() error {
			r.process(gctx, f)
			n := r.done.Add(1)
			fmt.Printf("\rОбработка: %d/%d", n, r.total)
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}
	fmt.Println()
	return nil
}

// gather lists files to process (copying to --output first when needed).
func (r *Runner) gather(ctx context.Context) ([]string, error) {
	src := r.cfg.Input
	var out []string
	err := filepath.WalkDir(src, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return nil // don't abort on a single unreadable entry
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if safefs.IsTempName(p) || isOurArtifact(p, r.cfg.Database) {
			return nil
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
	// copy media to output, then process the copies (original untouched)
	var dest []string
	for _, p := range out {
		res, err := detect.Detect(p)
		if err != nil || res.Kind == detect.KindUnknown {
			continue
		}
		rel, err := filepath.Rel(src, p)
		if err != nil {
			continue
		}
		dpath := filepath.Join(r.cfg.Output, rel)
		// Incremental output mode: if the honest final output already exists
		// (complete — swaps are atomic renames), this input is already done:
		// re-copying would resurrect raw originals and force a full re-encode.
		if final := r.finalPath(dpath, res); existsFile(final) {
			r.skipDone.Add(1)
			r.addSize(p, final)
			continue
		}
		if err := os.MkdirAll(filepath.Dir(dpath), 0o755); err != nil {
			return nil, err
		}
		if err := safefs.CopyFileBytes(p, dpath); err != nil {
			log.Printf("copy %s: %v", p, err)
			continue
		}
		dest = append(dest, dpath)
	}
	return dest, nil
}

func existsFile(p string) bool {
	st, err := os.Stat(p)
	return err == nil && !st.IsDir()
}

func (r *Runner) process(ctx context.Context, path string) {
	res, err := detect.Detect(path)
	if err != nil {
		r.errored.Add(1)
		logf(r.cfg, "detect %s: %v", path, err)
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
		logf(r.cfg, "hash %s: %v", path, err)
		return
	}

	// Idempotency: decide using the recorded outcome + settings stamp.
	rec, processed, err := r.store.IsProcessed(ctx, h)
	if err != nil {
		r.errored.Add(1)
		logf(r.cfg, "db lookup %s: %v", path, err)
		return
	}
	if processed {
		switch {
		case rec.Outcome == store.OutcomeNoChange:
			// Could not shrink before. Same settings -> skip; changed settings -> re-evaluate.
			if rec.Stamp == r.stampFor(res.Kind) {
				r.skipDone.Add(1)
				r.addSize(path, path)
				return
			}
		default: // compressed / legacy output already on disk
			if !r.legacyNeedsMigrate(h) {
				r.skipDone.Add(1)
				r.addSize(path, path)
				return
			}
		}
	}

	rel := r.rel(path)

	// Dry run: just report what would change.
	if r.cfg.DryRun {
		fmt.Printf("[dry-run] %s -> %s\n", rel, filepath.Base(r.finalPath(path, res)))
		return
	}

	r.encodeSwap(ctx, path, rel, h, res)
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

func (r *Runner) encodeSwap(ctx context.Context, path, rel, srcHash string, res detect.Result) {
	finalPath := r.finalPath(path, res) // honest extension target
	tmp, err := safefs.TempName(finalPath)
	if err != nil {
		r.errored.Add(1)
		logf(r.cfg, "temp %s: %v", path, err)
		return
	}

	var encErr error
	var origSize int64
	if oi, err := os.Stat(path); err == nil {
		origSize = oi.Size()
	}
	switch res.Kind {
	case detect.KindImage:
		encErr = media.EncodeImage(ctx, r.loc, r.opts, path, tmp)
	case detect.KindAnimated:
		encErr = media.EncodeAnimated(ctx, r.loc, r.opts, path, tmp)
	case detect.KindVideo:
		encErr = media.EncodeVideo(ctx, r.loc, r.opts, r.vchain, path, tmp, origSize)
	default:
		encErr = errors.New("unsupported media kind")
	}
	if errors.Is(encErr, media.ErrNotSmaller) {
		safefs.RemoveIfExists(tmp)
		r.skipNotSmaller.Add(1)
		r.addSize(path, path)
		_ = r.store.MarkNoChange(ctx, srcHash, rel, res.Kind.String(), mediaFormatFor(res, finalPath), toolSelf, toolVersion, r.stampFor(res.Kind))
		return
	}
	if encErr != nil {
		safefs.RemoveIfExists(tmp)
		r.errored.Add(1)
		logf(r.cfg, "encode %s: %v", rel, encErr)
		return
	}

	// Transfer metadata into the temp (so the final file carries the originals).
	if err := meta.TransferTags(ctx, r.exif, path, tmp); err != nil {
		if !errors.Is(err, meta.ErrNoExiftool) {
			logf(r.cfg, "meta %s: %v", rel, err)
		} else {
			logf(r.cfg, "meta: exiftool не найден — метаданные могут потеряться: %s", rel)
		}
	}
	// Restore original filesystem times before the swap.
	_ = meta.CopyFileTimes(path, tmp)

	if err := safefs.SyncFile(tmp); err != nil {
		safefs.RemoveIfExists(tmp)
		r.errored.Add(1)
		logf(r.cfg, "fsync %s: %v", rel, err)
		return
	}
	newHash, err := hashFile(tmp)
	if err != nil {
		safefs.RemoveIfExists(tmp)
		r.errored.Add(1)
		logf(r.cfg, "hash tmp %s: %v", rel, err)
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
		return
	}

	// Journal, then atomic replace, then commit — recoverable at every step.
	sid, err := r.store.BeginSwap(ctx, srcHash, newHash, finalPath, tmp)
	if err != nil {
		safefs.RemoveIfExists(tmp)
		r.errored.Add(1)
		logf(r.cfg, "begin swap %s: %v", rel, err)
		return
	}
	if err := safefs.AtomicReplace(tmp, finalPath); err != nil {
		_ = r.store.DropStaged(ctx, sid)
		safefs.RemoveIfExists(tmp)
		r.errored.Add(1)
		logf(r.cfg, "rename %s: %v", rel, err)
		return
	}
	newRel := r.rel(finalPath)
	if err := r.store.CommitSwap(ctx, sid, newHash, newRel, res.Kind.String(), mediaFormatFor(res, finalPath), toolSelf, toolVersion); err != nil {
		r.errored.Add(1)
		logf(r.cfg, "commit %s: %v", rel, err)
		return
	}
	if r.cfg.Migrate && r.legacyNeedsMigrate(srcHash) {
		_ = r.store.MarkMigrated(ctx, srcHash)
	}

	// Restore NTFS creation time onto the swapped-in file (Windows; no-op elsewhere).
	if err := meta.CopyBirthTime(path, finalPath); err != nil {
		logf(r.cfg, "birthtime %s: %v", newRel, err)
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
	if finalPath != path {
		safefs.RemoveIfExists(path)
	}
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
		fmt.Printf("[dry-run] fix-ext: %s -> %s\n", r.rel(path), r.rel(newPath))
		return
	}
	if err := os.Rename(path, newPath); err != nil {
		r.errored.Add(1)
		logf(r.cfg, "fix-ext %s: %v", r.rel(path), err)
		return
	}
	// update stored filename mapping for the known hash
	if h, err := hashFile(newPath); err == nil {
		_ = r.store.MarkCompressed(context.Background(), h, r.rel(newPath), res.Kind.String(), string(res.Format), "fix-ext", toolVersion)
	}
	r.processed.Add(1)
	r.addSize(path, newPath)
}

func (r *Runner) finalize(ctx context.Context, sid int64, finalPath, expectedHash string) error {
	res, err := detect.Detect(finalPath)
	if err != nil {
		return r.store.DropStaged(ctx, sid)
	}
	return r.store.CommitSwap(ctx, sid, expectedHash, r.rel(finalPath), res.Kind.String(), mediaFormatFor(res, finalPath), toolSelf, toolVersion)
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
		"Готово. Сжато/заменено: %d, уже обработано: %d, не уменьшилось: %d, не-медиа: %d, ошибок: %d\n"+
			"Размер: %.2f MB -> %.2f MB (%.2f%%)",
		r.processed.Load(), r.skipDone.Load(), r.skipNotSmaller.Load(), r.skipUnknown.Load(), r.errored.Load(),
		ob, nb, pct,
	)
}

func chainString(v media.VideoChain) string {
	c := v.Codecs()
	if len(c) == 0 {
		return "нет доступного AV1-кодировщика (проверь ffmpeg/драйверы)"
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

func replaceExt(path, ext string) string {
	base := path[:len(path)-len(filepath.Ext(path))]
	return base + ext
}

// isOurArtifact skips the tool's own database (and WAL/SHM sidecars).
func isOurArtifact(p, dbPath string) bool {
	base := filepath.Base(p)
	if base == "image_compressor.db" || base == "image_compressor.db-wal" || base == "image_compressor.db-shm" {
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
