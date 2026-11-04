package runner

import (
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/Llloooggg/ImageCompressor/internal/config"
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
