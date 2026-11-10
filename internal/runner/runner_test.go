package runner

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
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

// An honest name owned by a DIFFERENT file must not be taken over, and the lie
// must still be fixed (canon: honesty always): the liar moves to a numbered
// honest name and both files survive.
func TestEnsureHonestExt_Collision_TakesNumberedHonestName(t *testing.T) {
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
	got := r.ensureHonestExt(ctx, liar, res)

	if got == liar || got == honest {
		t.Fatalf("want a numbered honest name, got %s", filepath.Base(got))
	}
	if base := filepath.Base(got); base != "same (2).jpg" {
		t.Fatalf("want same (2).jpg, got %s", base)
	}
	if _, err := os.Stat(got); err != nil {
		t.Fatalf("the renamed honest file must exist: %v", err)
	}
	if _, err := os.Stat(liar); !os.IsNotExist(err) {
		t.Fatal("the lying name must be gone after the rename")
	}
	// The other file is untouched, byte for byte.
	data, err := os.ReadFile(honest)
	if err != nil || string(data) != "real photo" {
		t.Fatalf("the rival's file was damaged: %q %v", data, err)
	}
	if r.fixedExt.Load() != 1 {
		t.Fatalf("fixedExt = %d, want 1 (honesty was fixed)", r.fixedExt.Load())
	}
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

// One honest name must never be wanted by two different files: the FData
// archive lost 274 files that way (160 of them photos replaced by a frame
// pulled out of a same-named clip). claimTarget reserves the honest name when
// it is free and a NUMBERED name when a rival owns it, so both sources still
// get compressed and neither file is ever overwritten.
func TestClaimTarget_NumberedNameInsteadOfOverwrite(t *testing.T) {
	r, _ := newTestRunner(t)
	root := r.processRoot

	taken := filepath.Join(root, "x.avif")
	if err := os.WriteFile(taken, []byte("someone else's output"), 0o644); err != nil {
		t.Fatal(err)
	}
	src1 := filepath.Join(root, "x.jpg")
	chosen1, release1 := r.claimTarget(src1, taken)
	if chosen1 == taken {
		release1()
		t.Fatal("a name owned by another file must not be claimed")
	}
	defer release1()
	if got := filepath.Base(chosen1); got != "x (2).avif" {
		t.Fatalf("want the numbered honest name x (2).avif, got %s", got)
	}

	// Same stem, second source in flight: it must not take the first one's
	// numbered name either (that would swallow the in-flight output).
	src2 := filepath.Join(root, "x.webp")
	chosen2, release2 := r.claimTarget(src2, taken)
	if release2 != nil {
		defer release2()
	}
	if got := filepath.Base(chosen2); got != "x (3).avif" {
		t.Fatalf("a concurrent claim must move on to x (3).avif, got %s", got)
	}

	// A same-name swap (mp4 -> mp4) replaces its own source: no numbering.
	self := filepath.Join(root, "y.mp4")
	if chosen, release := r.claimTarget(self, self); chosen != self {
		release()
		t.Fatalf("same-name swap must keep its own name, got %s", chosen)
	} else {
		release()
	}

	// A released name is reusable: after the first swap lands, the next
	// claimant of the free honest name gets the honest name, not a number.
	free := filepath.Join(root, "z.avif")
	chosenA, releaseA := r.claimTarget(filepath.Join(root, "z.png"), free)
	if filepath.Base(chosenA) != "z.avif" {
		releaseA()
		t.Fatalf("a free target must be claimed as-is, got %s", chosenA)
	}
	releaseA()
	if err := os.WriteFile(free, []byte("our output"), 0o644); err != nil {
		t.Fatal(err)
	}
	chosenB, releaseB := r.claimTarget(filepath.Join(root, "z.webp"), free)
	defer releaseB()
	if got := filepath.Base(chosenB); got != "z (2).avif" {
		t.Fatalf("after the honest name landed, want z (2).avif, got %s", got)
	}

	if runtime.GOOS == "windows" {
		// NTFS folds case: IMAGE.PNG and image.jpg both want image.avif.
		upper := filepath.Join(root, "CASE.avif")
		if err := os.WriteFile(upper, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
		chosen, release := r.claimTarget(filepath.Join(root, "case.png"), filepath.Join(root, "case.avif"))
		defer release()
		if samePath(chosen, upper) {
			t.Fatal("a case-only collision with another file must be given a numbered name")
		}
		if got := filepath.Base(chosen); got != "case (2).avif" {
			t.Fatalf("want case (2).avif on a case-folded clash, got %s", got)
		}

		// clip.MP4 -> clip.mp4 is the SAME file (case-only honesty fix), not a
		// clash: no numbering, no second file. Getting this wrong is how an
		// uppercase-extension clip lost its own fresh output.
		upperSrc := filepath.Join(root, "UPPER.MP4")
		if err := os.WriteFile(upperSrc, []byte("clip"), 0o644); err != nil {
			t.Fatal(err)
		}
		chosen2, release2 := r.claimTarget(upperSrc, filepath.Join(root, "upper.mp4"))
		defer release2()
		if !samePath(chosen2, upperSrc) {
			t.Fatalf("case-only same-file target must stay itself, got %s", chosen2)
		}
	}
}

// The ceiling on the numbered-name search is a runaway guard, not a practical
// limit (the archive's real name fights are TWO files deep), so a deep chain
// must keep looking instead of giving up at an old small bound - and when the
// ceiling really is reached, the file is left exactly as it is.
func TestClaimTarget_DeepChainThenCeiling(t *testing.T) {
	r, _ := newTestRunner(t)
	root := r.processRoot

	honest := filepath.Join(root, "x.avif")
	if err := os.WriteFile(honest, []byte("first owner"), 0o644); err != nil {
		t.Fatal(err)
	}
	// occupy far more numbered names than the old ceiling of 64 allowed
	for n := 2; n <= 90; n++ {
		p := filepath.Join(root, fmt.Sprintf("x (%d).avif", n))
		if err := os.WriteFile(p, []byte("taken"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	chosen, release := r.claimTarget(filepath.Join(root, "x.jpg"), honest)
	if filepath.Base(chosen) != "x (91).avif" {
		release()
		t.Fatalf("a deep chain must keep counting, want x (91).avif, got %s", filepath.Base(chosen))
	}
	release()

	// fill up to the ceiling: nothing is claimed, nothing is overwritten
	for n := 91; n <= maxNameAttempts; n++ {
		p := filepath.Join(root, fmt.Sprintf("x (%d).avif", n))
		if err := os.WriteFile(p, []byte("taken"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	deep, releaseDeep := r.claimTarget(filepath.Join(root, "x.webp"), honest)
	releaseDeep()
	if deep != "" {
		t.Fatalf("at the ceiling no name may be claimed, got %s", filepath.Base(deep))
	}
	if data, err := os.ReadFile(honest); err != nil || string(data) != "first owner" {
		t.Fatalf("the ceiling must never damage the owner's file: %q %v", data, err)
	}
}

// shotPath is the naming half of the "hat" rule (user): EVERY collapsed
// frame carries the "_shot" postfix, no collision-testing needed — an
// original can never own an "_shot" name next to its picture (the filesystem
// forbids two files with one name), so the honest stem always stays with the
// real photo. Names with interior dots must survive untouched: the postfix
// rides at the end of the stem.
func TestShotPathAlwaysPostfixes(t *testing.T) {
	cases := map[string]string{
		filepath.Join("people", "x.avif"):                      filepath.Join("people", "x_shot.avif"),
		filepath.Join("a", "___alaska.___-20250606-0001.avif"): filepath.Join("a", "___alaska.___-20250606-0001_shot.avif"),
		filepath.Join("b", "holiday.photo (2024).avif"):        filepath.Join("b", "holiday.photo (2024)_shot.avif"),
	}
	for in, want := range cases {
		if got := shotPath(in); got != want {
			t.Errorf("shotPath(%q) = %q, want %q", in, got, want)
		}
	}
}

// The journal wording of a hat row must match what the run actually did. A hat
// never lands in the "not smaller" branch (the collapse does not bargain with
// size), so hatNote is a prefix only — inventing a decline sentence stamped 150
// rows that had just saved up to 80% of their weight with "one kept frame NOT
// SMALLER than the clip".
func TestHatNoteWording(t *testing.T) {
	if got := hatNote("", true); got != "" {
		t.Errorf("hatNote on a single-step collapse = %q, want empty", got)
	}
	if got := hatNote("avifenc q85 kept 1.2s", true); got != "hat: avifenc q85 kept 1.2s" {
		t.Errorf("hatNote with a trail = %q", got)
	}
	if got := hatNote("svt crf28 kept 40s", false); got != "svt crf28 kept 40s" {
		t.Errorf("hatNote for a plain video = %q, want the trail untouched", got)
	}
}

// The one real name fight left after the "_shot" rule: two hats sharing one
// stem ("x.mp4" + "x.mov") whose frames both want "x_shot.avif". claimTarget
// numbers the second frame ("x_shot (2).avif") instead of overwriting the
// first — the same guard photos get, just on the shot name.
func TestClaimTarget_ShotFrameFightGetsNumbered(t *testing.T) {
	r, _ := newTestRunner(t)
	root := r.processRoot
	shot := filepath.Join(root, "x_shot.avif")
	if err := os.WriteFile(shot, []byte("first frame"), 0o644); err != nil {
		t.Fatal(err)
	}
	chosen, release := r.claimTarget(filepath.Join(root, "x.mov"), shot)
	defer release()
	if got := filepath.Base(chosen); got != "x_shot (2).avif" {
		t.Fatalf("a second hat frame must get a numbered shot name, got %s", got)
	}
	if data, err := os.ReadFile(shot); err != nil || string(data) != "first frame" {
		t.Fatalf("the first frame must never be touched: %q %v", data, err)
	}
}

// End to end: a real photo "x.jpg" and a same-named "hat" clip coexist safely.
// The photo takes the honest "x.avif", the frame takes "x_shot.avif", nobody
// overwrites anybody — regardless of which file is processed first. (The old
// stemTwins gate blocked the collapse itself on a same-named sibling; that
// assumption died with a PSNR census of the live archive: 175 of 177 such
// pairs are DIFFERENT pictures.)
func TestProcess_HatNextToPhoto_ShotNameKeepsBoth(t *testing.T) {
	ctx := context.Background()
	loc := tools.NewLocator()
	if loc.FFmpeg() == "" {
		t.Skip("no ffmpeg (embedded or PATH), hat collapse test skipped")
	}
	// newTestRunner's config leaves Still=false; this test needs the collapse
	// switch on (production default is on, flags layer sets it).
	dir := t.TempDir()
	st, err := store.Open(context.Background(), filepath.Join(dir, "image_compressor.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	r := New(&config.Config{Input: dir, Yes: true, Still: true}, st, loc)
	r.processRoot = dir
	root := dir

	// Hat clip: one static gray frame + glued soundtrack, built the way
	// social apps do it (same fixture shape as media/still_test).
	hp := filepath.Join(root, "x.mp4")
	if b, err := exec.Command(loc.FFmpeg(), "-y", "-hide_banner", "-loglevel", "error",
		"-f", "lavfi", "-i", "color=c=0x7f807f:s=320x240:d=4:r=30",
		"-f", "lavfi", "-i", "sine=frequency=220:duration=4",
		"-c:v", "libx264", "-preset", "ultrafast", "-crf", "30", "-g", "120",
		"-pix_fmt", "yuv420p", "-c:a", "aac", "-shortest", hp).CombinedOutput(); err != nil {
		t.Fatalf("build hat clip: %v\n%s", err, b)
	}
	// A REAL photo with the same stem: a different picture (red frame).
	jp := filepath.Join(root, "x.jpg")
	if b, err := exec.Command(loc.FFmpeg(), "-y", "-hide_banner", "-loglevel", "error",
		"-f", "lavfi", "-i", "color=c=red:s=320x240:r=30", "-frames:v", "1", jp).CombinedOutput(); err != nil {
		t.Fatalf("build photo: %v\n%s", err, b)
	}

	r.process(ctx, hp) // hat first: the deadly order of the 05.11 run
	r.process(ctx, jp)

	if _, err := os.Stat(filepath.Join(root, "x_shot.avif")); err != nil {
		t.Fatalf("frame output x_shot.avif missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "x.avif")); err != nil {
		t.Fatalf("photo output x.avif missing: %v", err)
	}
	for _, gone := range []string{hp, jp} {
		if _, err := os.Stat(gone); !os.IsNotExist(err) {
			t.Fatalf("source %s must be replaced by its output", filepath.Base(gone))
		}
	}
	// No numbered junk beside them: the shot name left no fight for claimTarget.
	if es, err := os.ReadDir(root); err == nil {
		for _, e := range es {
			if strings.Contains(e.Name(), " (") {
				t.Fatalf("unexpected numbered file: %s", e.Name())
			}
		}
	}
	if r.errored.Load() != 0 {
		t.Fatalf("errored = %d, want 0", r.errored.Load())
	}

	// Second pass: both outputs are processed — idempotent, nothing moves.
	r.process(ctx, filepath.Join(root, "x_shot.avif"))
	r.process(ctx, filepath.Join(root, "x.avif"))
	for _, keep := range []string{"x_shot.avif", "x.avif"} {
		if _, err := os.Stat(filepath.Join(root, keep)); err != nil {
			t.Fatalf("second pass must leave %s alone: %v", keep, err)
		}
	}
	if r.errored.Load() != 0 {
		t.Fatalf("errored after second pass = %d, want 0", r.errored.Load())
	}
}

// The settings stamp of a "could not shrink" verdict must be the one the
// idempotency check will recompute for the kind the BYTES report. A "hat"
// collapse rewrites the kind to image, so stamping the rewritten kind wrote a
// row that could never match again and the file was re-encoded on every run
// (battle: azart_zay\azart_zay-20230407-0001.mp4 in every journal section).
func TestNoChangeStampMatchesTheKindTheCheckUses(t *testing.T) {
	ctx := context.Background()
	r, st := newTestRunner(t)
	const h = "hat-clip-hash"
	const rel = `azart_zay\azart_zay-20230407-0001.mp4`

	// What encodeSwap now writes for a refused hat: the ORIGINAL kind's stamp.
	if err := st.MarkNoChange(ctx, h, rel, detect.KindVideo.String(), r.stampFor(detect.KindVideo)); err != nil {
		t.Fatal(err)
	}
	rec, processed, err := st.IsProcessed(ctx, h)
	if err != nil || !processed {
		t.Fatalf("row must exist: processed=%v err=%v", processed, err)
	}
	// process() asks with the detected kind (a .mp4 is video).
	if rec.Stamp != r.stampFor(detect.KindVideo) {
		t.Fatalf("stamp must match the detected kind, got %q want %q", rec.Stamp, r.stampFor(detect.KindVideo))
	}

	// The old bug for contrast: an image-kind stamp can never match, which is
	// exactly the per-run re-encode loop.
	if r.stampFor(detect.KindImage) == r.stampFor(detect.KindVideo) {
		t.Fatal("photo and video stamps must differ, or the loop was never real")
	}
}
