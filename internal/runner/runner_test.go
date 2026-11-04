package runner

import (
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
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
	if err := st.MarkNoChange(ctx, h, "liar.dat", "image", "avif", toolSelf, toolVersion, r.stampFor(detect.KindImage)); err != nil {
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
	r.ensureHonestExt(liar, res)

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

// When the output dir lives inside the input dir, its contents must never be
// walked as inputs (previous mirror resurrecting as fresh source).
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
