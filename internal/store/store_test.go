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
	if err := s.MarkCompressed(ctx, "h1", "a.jpg", "image", "avif", "self", "1"); err != nil {
		t.Fatal(err)
	}
	rec, ok, _ := s.IsProcessed(ctx, "h1")
	if !ok {
		t.Fatal("h1 should be processed after MarkCompressed")
	}
	if rec.Outcome != OutcomeCompressed {
		t.Fatalf("outcome want compressed, got %q", rec.Outcome)
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
	if err := s.CommitSwap(ctx, id, "newH", "final.avif", "image", "avif", "self", "1"); err != nil {
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

// TestLegacyAdoption verifies Open adopts a legacy (hash+filename only) table,
// back-fills tool='legacy-python', and surfaces it as an unmigrated hash.
func TestLegacyAdoption(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "image_compressor.db")

	// Create the legacy schema by hand (Python-era: hash, filename only).
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

	// Seeded for idempotency: the legacy hash is known, so Go won't re-touch it.
	if _, ok, _ := s.IsProcessed(ctx, "legacyhash"); !ok {
		t.Fatal("legacy hash should be recognized (idempotency)")
	}
	// But it is flagged as unmigrated legacy output for --migrate.
	set, err := s.LegacyUnmigratedHashes(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := set["legacyhash"]; !ok {
		t.Fatal("legacyhash should be in LegacyUnmigratedHashes")
	}
	// After migrate mark, it leaves the unmigrated set.
	if err := s.MarkMigrated(ctx, "legacyhash"); err != nil {
		t.Fatal(err)
	}
	set, _ = s.LegacyUnmigratedHashes(ctx)
	if _, ok := set["legacyhash"]; ok {
		t.Fatal("after MarkMigrated it must not be unmigrated")
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

	if err := s.MarkNoChange(ctx, "srcH", "a.mp4", "video", "mp4", "self", "1", "vid|24|auto|ff=true"); err != nil {
		t.Fatal(err)
	}
	rec, ok, _ := s.IsProcessed(ctx, "srcH")
	if !ok || rec.Outcome != OutcomeNoChange || rec.Stamp != "vid|24|auto|ff=true" {
		t.Fatalf("unexpected record: ok=%v outcome=%q stamp=%q", ok, rec.Outcome, rec.Stamp)
	}
	// A changed settings stamp overwrites the cached decision.
	if err := s.MarkNoChange(ctx, "srcH", "a.mp4", "video", "mp4", "self", "1", "vid|20|auto|ff=true"); err != nil {
		t.Fatal(err)
	}
	rec, _, _ = s.IsProcessed(ctx, "srcH")
	if rec.Stamp != "vid|20|auto|ff=true" {
		t.Fatalf("stamp should update on re-evaluation, got %q", rec.Stamp)
	}
}
