package runner

import (
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/Llloooggg/ImageCompressor/internal/config"
	"github.com/Llloooggg/ImageCompressor/internal/detect"
	"github.com/Llloooggg/ImageCompressor/internal/store"
	"github.com/Llloooggg/ImageCompressor/internal/tools"
)

// minimal ftyp box so detect classifies the swapped file as AVIF (KindImage).
func avifBytes() []byte {
	out := make([]byte, 24)
	binary.BigEndian.PutUint32(out[0:4], 24)
	copy(out[4:8], "ftyp")
	copy(out[8:12], "avif")
	copy(out[16:20], "avif")
	copy(out[20:24], "mif1")
	return out
}

func newTestRunner(t *testing.T) (*Runner, *store.Store) {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	st, err := store.Open(ctx, filepath.Join(dir, "image_compressor.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	r := New(&config.Config{Input: dir, Yes: true}, st, tools.NewLocator())
	r.processRoot = dir
	return r, st
}

func TestRecover_TmpMatches_CompletesSwap(t *testing.T) {
	ctx := context.Background()
	r, st := newTestRunner(t)

	tmp := filepath.Join(r.processRoot, "photo.mc-tmp-x.avif")
	final := filepath.Join(r.processRoot, "photo.avif")
	data := avifBytes()
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		t.Fatal(err)
	}
	wantHash, _ := hashFile(tmp)

	id, err := st.BeginSwap(ctx, "orig", wantHash, final, tmp)
	if err != nil {
		t.Fatal(err)
	}

	if err := r.Recover(ctx); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(final); err != nil {
		t.Fatal("final should exist after completing interrupted swap")
	}
	if _, err := os.Stat(tmp); !os.IsNotExist(err) {
		t.Fatal("tmp should be gone after rename")
	}
	if _, ok, _ := st.IsProcessed(ctx, wantHash); !ok {
		t.Fatal("swapped file hash should be processed")
	}
	rows, _ := st.PendingStaged(ctx)
	if len(rows) != 0 {
		t.Fatalf("staged should be cleared, got %d", len(rows))
	}
	_ = id
}

func TestRecover_FinalAlreadyLanded_Finalizes(t *testing.T) {
	ctx := context.Background()
	r, st := newTestRunner(t)

	final := filepath.Join(r.processRoot, "done.avif")
	tmp := filepath.Join(r.processRoot, "gone.mc-tmp.avif")
	if err := os.WriteFile(final, avifBytes(), 0o644); err != nil {
		t.Fatal(err)
	}
	wantHash, _ := hashFile(final)

	if _, err := st.BeginSwap(ctx, "orig", wantHash, final, tmp); err != nil {
		t.Fatal(err)
	}
	if err := r.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := st.IsProcessed(ctx, wantHash); !ok {
		t.Fatal("already-landed file should be finalized as processed")
	}
	rows, _ := st.PendingStaged(ctx)
	if len(rows) != 0 {
		t.Fatalf("staged should be cleared, got %d", len(rows))
	}
}

func TestRecover_TmpGarbage_IsDiscarded(t *testing.T) {
	ctx := context.Background()
	r, st := newTestRunner(t)

	tmp := filepath.Join(r.processRoot, "bad.mc-tmp.avif")
	final := filepath.Join(r.processRoot, "never.avif")
	if err := os.WriteFile(tmp, []byte("garbage not matching expected"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := st.BeginSwap(ctx, "orig", "deadbeef-hash", final, tmp); err != nil {
		t.Fatal(err)
	}
	if err := r.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(tmp); !os.IsNotExist(err) {
		t.Fatal("garbage tmp must be removed")
	}
	if _, err := os.Stat(final); !os.IsNotExist(err) {
		t.Fatal("no bogus final should be created")
	}
	rows, _ := st.PendingStaged(ctx)
	if len(rows) != 0 {
		t.Fatalf("staged should be dropped, got %d", len(rows))
	}
}

// Output mode must mirror fs timestamps of the originals onto the staged
// copies (rule 5): CopyFileBytes moves bytes only, so gather has to carry
// the times over explicitly.
func TestGather_OutputMode_CopiesPreserveTimes(t *testing.T) {
	ctx := context.Background()
	in := t.TempDir()
	out := t.TempDir()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "image_compressor.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })

	orig := filepath.Join(in, "a.jpg")
	jpeg := []byte{0xFF, 0xD8, 0xFF, 0xE0, 0, 0, 0, 0, 0xFF, 0xD9}
	if err := os.WriteFile(orig, jpeg, 0o644); err != nil {
		t.Fatal(err)
	}
	past := time.Now().Add(-72 * time.Hour).Truncate(time.Second)
	if err := os.Chtimes(orig, past, past); err != nil {
		t.Fatal(err)
	}

	r := New(&config.Config{Input: in, Output: out, Yes: true}, st, tools.NewLocator())
	paths, err := r.gather(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 1 {
		t.Fatalf("want 1 staged copy, got %d", len(paths))
	}
	if paths[0] != filepath.Join(out, "a.jpg") {
		t.Fatalf("unexpected staged path: %s", paths[0])
	}
	fi, err := os.Stat(paths[0])
	if err != nil {
		t.Fatal(err)
	}
	if !fi.ModTime().Equal(past) {
		t.Fatalf("mtime not preserved: got %v want %v", fi.ModTime(), past)
	}
}

// Rule (user): lying extensions are fixed ALWAYS — even for files we decided
// not to compress (NoChange record with a matching settings stamp).
func TestProcess_NoChange_LyingExt_StillRenamed(t *testing.T) {
	ctx := context.Background()
	r, st := newTestRunner(t)

	jpeg := []byte{0xFF, 0xD8, 0xFF, 0xE0, 0, 0, 0, 0, 0xFF, 0xD9}
	path := filepath.Join(r.processRoot, "liar.dat") // jpeg bytes, lying ext
	if err := os.WriteFile(path, jpeg, 0o644); err != nil {
		t.Fatal(err)
	}
	h, err := hashFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.MarkNoChange(ctx, h, "liar.dat", "image", r.stampFor(detect.KindImage)); err != nil {
		t.Fatal(err)
	}

	r.process(ctx, path)

	if _, err := os.Stat(filepath.Join(r.processRoot, "liar.jpg")); err != nil {
		t.Fatalf("honest name missing after nochange skip: %v", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("lying name must be gone after the rename")
	}
	if r.fixedExt.Load() != 1 {
		t.Fatalf("fixedExt = %d, want 1", r.fixedExt.Load())
	}
	if r.skipDone.Load() != 1 {
		t.Fatalf("skipDone = %d, want 1", r.skipDone.Load())
	}
}

// Collision with an existing honest name -> keep the file, warn, no rename.
func TestEnsureHonestExt_Collision_KeepsFile(t *testing.T) {
	ctx := context.Background()
	r, _ := newTestRunner(t)

	jpeg := []byte{0xFF, 0xD8, 0xFF, 0xE0, 0, 0, 0, 0, 0xFF, 0xD9}
	liar := filepath.Join(r.processRoot, "same.dat")
	honest := filepath.Join(r.processRoot, "same.jpg")
	if err := os.WriteFile(liar, jpeg, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(honest, []byte("real photo"), 0o644); err != nil {
		t.Fatal(err)
	}

	res, err := detect.Detect(liar)
	if err != nil {
		t.Fatal(err)
	}
	r.ensureHonestExt(ctx, liar, res)

	if _, err := os.Stat(liar); err != nil {
		t.Fatal("colliding file must stay in place")
	}
	if r.fixedExt.Load() != 0 {
		t.Fatalf("fixedExt = %d, want 0 on collision", r.fixedExt.Load())
	}
	_ = ctx
}

// Output mode must be a FULL mirror: non-media files land in the output too,
// and a second gather reuses already-mirrored copies instead of re-copying.
func TestGather_OutputMode_FullMirror(t *testing.T) {
	ctx := context.Background()
	in, out := t.TempDir(), t.TempDir()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "image_compressor.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })

	jpeg := []byte{0xFF, 0xD8, 0xFF, 0xE0, 0, 0, 0, 0, 0xFF, 0xD9}
	if err := os.WriteFile(filepath.Join(in, "a.jpg"), jpeg, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(in, "notes.txt"), []byte("keep me"), 0o644); err != nil {
		t.Fatal(err)
	}
	sub := filepath.Join(in, "sub")
	if err := os.MkdirAll(sub, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sub, "archive.bin"), []byte{1, 2, 3}, 0o644); err != nil {
		t.Fatal(err)
	}

	r := New(&config.Config{Input: in, Output: out, Yes: true}, st, tools.NewLocator())
	paths, err := r.gather(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 3 {
		t.Fatalf("want 3 mirrored files, got %d: %v", len(paths), paths)
	}
	for _, want := range []string{
		filepath.Join(out, "a.jpg"),
		filepath.Join(out, "notes.txt"),
		filepath.Join(out, "sub", "archive.bin"),
	} {
		if _, err := os.Stat(want); err != nil {
			t.Fatalf("missing mirror copy %s: %v", want, err)
		}
	}

	// Second pass (incremental): still 3, files reused (no error, no dupes).
	r2 := New(&config.Config{Input: in, Output: out, Yes: true}, st, tools.NewLocator())
	paths2, err := r2.gather(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(paths2) != 3 {
		t.Fatalf("incremental gather: want 3, got %d: %v", len(paths2), paths2)
	}
}

// A previous run mirrored a liar and renamed it honestly; the next gather
// must reuse that honest copy instead of re-copying the raw liar under the
// lying name (which would collide on rename time).
func TestGather_OutputMode_HonestCopyReused(t *testing.T) {
	ctx := context.Background()
	in, out := t.TempDir(), t.TempDir()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "image_compressor.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })

	jpeg := []byte{0xFF, 0xD8, 0xFF, 0xE0, 0, 0, 0, 0, 0xFF, 0xD9}
	if err := os.WriteFile(filepath.Join(in, "liar.dat"), jpeg, 0o644); err != nil {
		t.Fatal(err)
	}
	// Simulate the state left by a previous run: honest copy in the mirror.
	if err := os.WriteFile(filepath.Join(out, "liar.jpg"), jpeg, 0o644); err != nil {
		t.Fatal(err)
	}

	r := New(&config.Config{Input: in, Output: out, Yes: true}, st, tools.NewLocator())
	paths, err := r.gather(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 1 {
		t.Fatalf("want 1 path, got %d: %v", len(paths), paths)
	}
	if paths[0] != filepath.Join(out, "liar.jpg") {
		t.Fatalf("want reuse of honest copy, got %s", paths[0])
	}
	if _, err := os.Stat(filepath.Join(out, "liar.dat")); !os.IsNotExist(err) {
		t.Fatal("raw liar must not be re-copied under the lying name")
	}
}

// Output inside input: previous mirror must never be walked back as input.
func TestGather_OutputInsideInput_Excluded(t *testing.T) {
	ctx := context.Background()
	in := t.TempDir()
	out := filepath.Join(in, "out")
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "image_compressor.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })

	if err := os.MkdirAll(out, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(out, "prev.txt"), []byte("mirror"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(in, "a.txt"), []byte("src"), 0o644); err != nil {
		t.Fatal(err)
	}

	r := New(&config.Config{Input: in, Output: out, Yes: true}, st, tools.NewLocator())
	paths, err := r.gather(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 1 || paths[0] != filepath.Join(out, "a.txt") {
		t.Fatalf("output subtree leaked into inputs: %v", paths)
	}
}

func TestReplaceExtChain(t *testing.T) {
	tests := []struct {
		path, ext, want string
	}{
		{"a.mp4.mov", ".mp4", "a.mp4"},                   // chained exts collapse into one
		{"b.jpg.jpg", ".avif", "b.avif"},                 // doubled ext fully replaced
		{"liar.dat", ".jpg", "liar.jpg"},                 // unknown ext dropped as before
		{"photo.final.jpg", ".avif", "photo.final.avif"}, // dots inside names stay
		{"bandicam 2021-07-21 22-08-04-365.mp4", ".mp4", "bandicam 2021-07-21 22-08-04-365.mp4"},
		{"plain", ".mp4", "plain.mp4"},
		{"VID_20260227_102325_800.mp4.mov", ".mp4", "VID_20260227_102325_800.mp4"},
	}
	for _, tt := range tests {
		if got := replaceExt(tt.path, tt.ext); got != tt.want {
			t.Errorf("replaceExt(%q, %q) = %q, want %q", tt.path, tt.ext, got, tt.want)
		}
	}
}

// Doubled media extension (".mp4.mp4" noise from earlier tools) must be
// cleaned to a single honest ext even when the file itself is not compressed.
func TestEnsureHonestExt_DoubledExt_Cleaned(t *testing.T) {
	r, _ := newTestRunner(t)

	// minimal ftyp box so detect classifies it as MP4 (KindVideo)
	mk := make([]byte, 24)
	binary.BigEndian.PutUint32(mk[0:4], 24)
	copy(mk[4:8], "ftyp")
	copy(mk[8:12], "isom")
	copy(mk[16:20], "isom")
	copy(mk[20:24], "mp41")

	path := filepath.Join(r.processRoot, "VID_2026.mp4.mp4")
	if err := os.WriteFile(path, mk, 0o644); err != nil {
		t.Fatal(err)
	}
	res, err := detect.Detect(path)
	if err != nil {
		t.Fatal(err)
	}
	if res.Format != detect.FmtMP4 {
		t.Fatalf("want mp4, got %v", res.Format)
	}
	if res.ExtensionLies {
		t.Fatal("ext not expected to lie here — the doubled chain alone must trigger the fix")
	}

	r.ensureHonestExt(context.Background(), path, res)

	if _, err := os.Stat(filepath.Join(r.processRoot, "VID_2026.mp4")); err != nil {
		t.Fatalf("clean name missing: %v", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("doubled name must be gone")
	}
	if r.fixedExt.Load() != 1 {
		t.Fatalf("fixedExt = %d, want 1", r.fixedExt.Load())
	}
}

// Cold hashing of big files must not read them all at once: the archive HDD
// thrashed at ~22 MB/s with 12 concurrent hashers (seek thrash). Small files
// and cache hits never wait for a read lane; a stop request must not block.
func TestHashLane_BigFilesOnly(t *testing.T) {
	r, _ := newTestRunner(t)
	ctx := context.Background()

	drain := func() {
		for {
			select {
			case <-r.hsem:
			default:
				return
			}
		}
	}
	fill := func() {
		for i := 0; i < cap(r.hsem); i++ {
			r.hsem <- struct{}{}
		}
	}
	t.Cleanup(drain)
	fill()

	// Small files ignore the lanes entirely.
	release, ok := r.acquireHashLane(ctx, "small", bigHashThreshold-1)
	if !ok {
		t.Fatal("small file must not wait for a lane")
	}
	release()

	// A big file waits while every lane is taken.
	done := make(chan bool, 1)
	go func() {
		rel, ok := r.acquireHashLane(ctx, "big", bigHashThreshold)
		if ok {
			rel()
		}
		done <- ok
	}()
	select {
	case <-done:
		t.Fatal("big file should wait while all lanes are busy")
	case <-time.After(50 * time.Millisecond):
	}
	<-r.hsem // free exactly one lane
	select {
	case ok := <-done:
		if !ok {
			t.Fatal("big file must acquire after a lane is freed")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("big file did not proceed after a lane was freed")
	}

	// Cancelled wait: all lanes busy + ctx already done -> no blocking read.
	drain()
	fill()
	ctxStop, cancel := context.WithCancel(ctx)
	cancel()
	if rel, ok := r.acquireHashLane(ctxStop, "big", bigHashThreshold); ok {
		rel()
		t.Fatal("cancelled wait must report ok=false")
	}
}

// Two DIFFERENT sources must never take one honest name. The FData archive
// lost 89 files this way ("a.jpg" + "a.webp" copies, "a.jpg" + "A.PNG" on a
// case-insensitive filesystem): the later swap overwrote the first output and
// removed its source. The claim makes the refusal explicit.
func TestClaimTarget_NeverOverwritesAnotherFile(t *testing.T) {
	r, _ := newTestRunner(t)

	taken := filepath.Join(r.processRoot, "x.avif")
	if err := os.WriteFile(taken, []byte("someone else's output"), 0o644); err != nil {
		t.Fatal(err)
	}
	if rel, ok := r.claimTarget(filepath.Join(r.processRoot, "x.jpg"), taken); ok {
		rel()
		t.Fatal("a target owned by another file must be refused")
	}

	// A same-name swap (mp4 -> mp4) replaces its own source: always allowed.
	self := filepath.Join(r.processRoot, "y.mp4")
	if rel, ok := r.claimTarget(self, self); !ok {
		rel()
		t.Fatal("same-name swap must be allowed")
	}

	// Free name: claimable once; a second worker asking for it loses until the
	// first releases.
	free := filepath.Join(r.processRoot, "z.avif")
	rel1, ok := r.claimTarget(filepath.Join(r.processRoot, "z.png"), free)
	if !ok {
		t.Fatal("a free target must be claimable")
	}
	if rel2, ok := r.claimTarget(filepath.Join(r.processRoot, "z.webp"), free); ok {
		rel2()
		t.Fatal("a target claimed by another worker must be refused")
	}
	rel1()
	rel3, ok := r.claimTarget(filepath.Join(r.processRoot, "z.webp"), free)
	if !ok {
		t.Fatal("a released target must be claimable again")
	}
	rel3()

	if runtime.GOOS == "windows" {
		// NTFS folds case: IMAGE.PNG and image.jpg both want image.avif.
		upper := filepath.Join(r.processRoot, "CASE.avif")
		if err := os.WriteFile(upper, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
		if rel, ok := r.claimTarget(filepath.Join(r.processRoot, "case.png"), filepath.Join(r.processRoot, "case.avif")); ok {
			rel()
			t.Fatal("case-only collision must be refused on Windows")
		}
	}
}
