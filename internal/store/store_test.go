package store

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"
)

func TestProcessedAndStaged(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "t.db")
	s, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if _, ok, _ := s.IsProcessed(ctx, "h1"); ok {
		t.Fatal("h1 should be unknown initially")
	}
	if err := s.MarkCompressed(ctx, "h1", "a.jpg", "image", "q=85"); err != nil {
		t.Fatal(err)
	}
	rec, ok, _ := s.IsProcessed(ctx, "h1")
	if !ok {
		t.Fatal("h1 should be processed after MarkCompressed")
	}
	if rec.Outcome != OutcomeCompressed {
		t.Fatalf("outcome want compressed, got %q", rec.Outcome)
	}
	if rec.Achieved != "q=85" {
		t.Fatalf("achieved want q=85, got %q", rec.Achieved)
	}

	// staged flow: begin -> pending -> commit
	id, err := s.BeginSwap(ctx, "srcH", "newH", "/abs/final.avif", "/abs/tmp.avif")
	if err != nil {
		t.Fatal(err)
	}
	rows, err := s.PendingStaged(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].ID != id {
		t.Fatalf("want 1 pending staged, got %d", len(rows))
	}
	if err := s.CommitSwap(ctx, id, "newH", "final.avif", "image", ""); err != nil {
		t.Fatal(err)
	}
	rows, _ = s.PendingStaged(ctx)
	if len(rows) != 0 {
		t.Fatalf("staged should be cleared, got %d", len(rows))
	}
	if _, ok, _ := s.IsProcessed(ctx, "newH"); !ok {
		t.Fatal("newH should be processed after CommitSwap")
	}
}

// TestLegacyTableAdoption verifies Open adopts an ancient (hash+filename only)
// table and its rows still protect their content from re-compression. There is
// no legacy tooling left in the code: the rows are just journal entries, and a
// full walk's GC prunes them once their content is gone from the tree.
func TestLegacyTableAdoption(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "image_compressor.db")

	raw, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = raw.Exec(`DROP TABLE IF EXISTS processed_images`)
	if _, err := raw.Exec(`CREATE TABLE processed_images (hash TEXT PRIMARY KEY, filename TEXT)`); err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Exec(`INSERT INTO processed_images(hash, filename) VALUES('legacyhash', 'old/out.jpg')`); err != nil {
		t.Fatal(err)
	}
	raw.Close()

	s, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if _, ok, _ := s.IsProcessed(ctx, "legacyhash"); !ok {
		t.Fatal("legacy hash should be recognized (idempotency)")
	}
	// A completed walk whose real content ("other") never matched it: GC sweeps it.
	seen := map[string]struct{}{"other": {}} // 1 seen vs 1 row -> not a partial walk
	n, guarded, err := s.PruneUnseen(ctx, seen)
	if err != nil || guarded {
		t.Fatalf("prune should run: n=%d guarded=%v err=%v", n, guarded, err)
	}
	if n != 1 {
		t.Fatalf("want 1 pruned, got %d", n)
	}
	if _, ok, _ := s.IsProcessed(ctx, "legacyhash"); ok {
		t.Fatal("unseen legacy hash must be pruned")
	}
}

// TestNoChangeStamp verifies the settings-sensitive "nochange" cache.
func TestNoChangeStamp(t *testing.T) {
	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "nc.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if err := s.MarkNoChange(ctx, "srcH", "a.mp4", "video", "vid|24|auto|ff=true"); err != nil {
		t.Fatal(err)
	}
	rec, ok, _ := s.IsProcessed(ctx, "srcH")
	if !ok || rec.Outcome != OutcomeNoChange || rec.Stamp != "vid|24|auto|ff=true" {
		t.Fatalf("unexpected record: ok=%v outcome=%q stamp=%q", ok, rec.Outcome, rec.Stamp)
	}
	// A changed settings stamp overwrites the cached decision.
	if err := s.MarkNoChange(ctx, "srcH", "a.mp4", "video", "vid|20|auto|ff=true"); err != nil {
		t.Fatal(err)
	}
	rec, _, _ = s.IsProcessed(ctx, "srcH")
	if rec.Stamp != "vid|20|auto|ff=true" {
		t.Fatalf("stamp should update on re-evaluation, got %q", rec.Stamp)
	}
}

// TestPruneUnseen covers the zombie GC: rows for content the walk never saw
// are deleted, seen ones survive, and the >50% guard protects a shared --db
// from a subfolder walk eating unrelated records.
func TestPruneUnseen(t *testing.T) {
	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "gc.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	for _, h := range []string{"gone1", "gone2", "kept1", "kept2", "kept3", "kept4"} {
		if err := s.MarkCompressed(ctx, h, h+".avif", "image", "q=85"); err != nil {
			t.Fatal(err)
		}
	}

	// Guard: only 2 of 6 rows seen -> under half -> refuse, delete nothing.
	seen := map[string]struct{}{"kept1": {}, "kept2": {}}
	n, guarded, err := s.PruneUnseen(ctx, seen)
	if err != nil || !guarded || n != 0 {
		t.Fatalf("want guarded refusal, got n=%d guarded=%v err=%v", n, guarded, err)
	}

	// Full walk: 5 of 6 seen (>= half) -> sweep the two zombies.
	seen["kept3"] = struct{}{}
	seen["kept4"] = struct{}{}
	seen["fresh-not-in-db"] = struct{}{} // observed content without a row: fine
	n, guarded, err = s.PruneUnseen(ctx, seen)
	if err != nil || guarded {
		t.Fatalf("prune failed: guarded=%v err=%v", guarded, err)
	}
	if n != 2 {
		t.Fatalf("want 2 pruned, got %d", n)
	}
	for _, h := range []string{"gone1", "gone2"} {
		if _, ok, _ := s.IsProcessed(ctx, h); ok {
			t.Fatalf("%s should have been pruned", h)
		}
	}
	for _, h := range []string{"kept1", "kept2", "kept3", "kept4"} {
		if _, ok, _ := s.IsProcessed(ctx, h); !ok {
			t.Fatalf("%s should have survived", h)
		}
	}

	// Second call must work too (temp table fully cleaned up every time).
	if _, guarded, err := s.PruneUnseen(ctx, seen); err != nil || guarded {
		t.Fatalf("repeat prune failed: guarded=%v err=%v", guarded, err)
	}
}

// TestSeenCache covers the re-read fast path: a path remembers its content
// hash under the exact (size, mtime) it was hashed with; any change in the
// triple is a miss and the real hash must be computed again. Renames move the
// key, and the GC drops cache rows whose content left the journal.
func TestSeenCache(t *testing.T) {
	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "seen.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	const (
		path  = `family\clip.mp4`
		hash  = "deadbeef"
		size  = int64(12345)
		mtime = int64(1_700_000_000_000_000_000)
	)
	if _, ok, err := s.SeenHash(ctx, path, size, mtime); err != nil || ok {
		t.Fatalf("empty cache must miss: ok=%v err=%v", ok, err)
	}
	if err := s.RememberPath(ctx, path, size, mtime, hash); err != nil {
		t.Fatal(err)
	}
	got, ok, err := s.SeenHash(ctx, path, size, mtime)
	if err != nil || !ok || got != hash {
		t.Fatalf("exact triple must hit: got=%q ok=%v err=%v", got, ok, err)
	}
	// Size or mtime drift -> the content may have changed -> no shortcut.
	if _, ok, _ := s.SeenHash(ctx, path, size+1, mtime); ok {
		t.Fatal("size change must miss")
	}
	if _, ok, _ := s.SeenHash(ctx, path, size, mtime+1); ok {
		t.Fatal("mtime change must miss")
	}
	// Unknown path is a miss, not an error.
	if _, ok, err := s.SeenHash(ctx, `family\other.mp4`, size, mtime); err != nil || ok {
		t.Fatalf("unknown path must miss: ok=%v err=%v", ok, err)
	}

	// Rename: the cached triple follows the file to the new name.
	const honest = `family\clip.mov`
	if err := s.RenamePath(ctx, path, honest); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := s.SeenHash(ctx, path, size, mtime); ok {
		t.Fatal("old path must be gone after rename")
	}
	if got, ok, _ := s.SeenHash(ctx, honest, size, mtime); !ok || got != hash {
		t.Fatal("renamed path must inherit the cache")
	}

	// Upsert: a new triple for the same path overwrites in place.
	if err := s.RememberPath(ctx, honest, size*2, mtime*2, hash); err != nil {
		t.Fatal(err)
	}
	if got, ok, _ := s.SeenHash(ctx, honest, size*2, mtime*2); !ok || got != hash {
		t.Fatalf("upsert failed: got=%q ok=%v", got, ok)
	}

	// GC: a cache row whose content left the journal is dead weight (the file
	// was restored/replaced outside the tool), while the row for live content
	// stays.
	if err := s.MarkCompressed(ctx, hash, honest, "video", "svt p8 crf=28"); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkCompressed(ctx, "live", `family\live.avif`, "image", "avifenc q=85"); err != nil {
		t.Fatal(err)
	}
	if err := s.RememberPath(ctx, `family\live.avif`, 1, 2, "live"); err != nil {
		t.Fatal(err)
	}
	if err := s.RememberPath(ctx, `family\stale.mp4`, 3, 4, "stale"); err != nil {
		t.Fatal(err)
	}
	// The walk observed both live hashes, so the >50% guard does not fire.
	if _, guarded, err := s.PruneUnseen(ctx, map[string]struct{}{hash: {}, "live": {}}); err != nil || guarded {
		t.Fatalf("prune failed: guarded=%v err=%v", guarded, err)
	}
	if _, ok, _ := s.SeenHash(ctx, `family\stale.mp4`, 3, 4); ok {
		t.Fatal("cache row for dead content must be pruned")
	}
	if _, ok, _ := s.SeenHash(ctx, honest, size*2, mtime*2); !ok {
		t.Fatal("cache row for live content must survive")
	}
	if _, ok, _ := s.SeenHash(ctx, `family\live.avif`, 1, 2); !ok {
		t.Fatal("cache row for live content must survive")
	}
}
