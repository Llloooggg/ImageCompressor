// Package store is the idempotency + crash-recovery backbone.
//
// Rules that keep a sensitive archive safe:
//  1. A file whose content hash is already recorded as processed is not touched
//     again — unless it was recorded as "nochange" (couldn't shrink) with a
//     DIFFERENT settings stamp, in which case we re-evaluate. This lets us skip
//     already-optimal files without re-encoding videos every run, while still
//     honouring changed --photo-q/--video-crf/encoder choices.
//  2. Swapping an encoded file over the original is a two-phase, journaled
//     operation (encode -> fsync -> journal BEGIN -> atomic rename -> DB COMMIT).
//     If the process dies at any point, Recover() finishes or undoes the swap.
//
// It also adopts the legacy Python image_compressor.db (table processed_images
// with hash+filename) on first run and adds its own columns non-destructively.
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

const driver = "sqlite"

// Outcome of a processed record.
const (
	OutcomeCompressed = "compressed" // we produced/own the canonical file on disk
	OutcomeNoChange   = "nochange"   // evaluated; could not shrink -> original kept
)

// Tool identifies who produced a stored hash.
const ToolLegacy = "legacy-python"

// Record is a processed file entry returned by IsProcessed.
type Record struct {
	Hash     string
	Filename string // legacy: "|" joined relative paths
	Media    string
	Format   string
	Tool     string
	Version  string
	Migrated int
	Outcome  string // compressed | nochange | "" (legacy unknown)
	Stamp    string // settings fingerprint for nochange decisions
}

// StagedRow is an in-flight swap that a crash may have interrupted.
type StagedRow struct {
	ID           int64
	SourceHash   string
	ExpectedHash string
	FinalPath    string
	TmpPath      string
}

// Store serializes all DB access (single-writer).
type Store struct {
	db *sql.DB
	mu sync.Mutex
}

// Open opens (or creates) the DB, adopts a legacy schema if present, and enables
// durability settings.
func Open(ctx context.Context, path string) (*Store, error) {
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}
	dsn := fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=busy_timeout(15000)&_pragma=synchronous(FULL)&_pragma=foreign_keys(1)", path)
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1) // single writer: WAL + this avoids BUSY under the pool

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, err
	}
	s := &Store{db: db}
	if err := s.migrate(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) migrate(ctx context.Context) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS processed_images (
			hash     TEXT PRIMARY KEY,
			filename TEXT
		);`,
		`CREATE TABLE IF NOT EXISTS staged (
			id            INTEGER PRIMARY KEY AUTOINCREMENT,
			source_hash   TEXT,
			expected_hash TEXT NOT NULL,
			final_path    TEXT NOT NULL,
			tmp_path      TEXT NOT NULL,
			created_at    TEXT NOT NULL
		);`,
	}
	for _, q := range stmts {
		if _, err := s.db.ExecContext(ctx, q); err != nil {
			return fmt.Errorf("migrate: %w", err)
		}
	}

	have, err := s.columns(ctx, "processed_images")
	if err != nil {
		return err
	}
	want := map[string]string{
		"media_kind":   "TEXT",
		"format":       "TEXT",
		"tool":         "TEXT",
		"tool_version": "TEXT",
		"migrated":     "INTEGER DEFAULT 0",
		"updated_at":   "TEXT",
		"outcome":      "TEXT",
		"stamp":        "TEXT",
	}
	for col, typ := range want {
		if !have[col] {
			if _, err := s.db.ExecContext(ctx,
				fmt.Sprintf("ALTER TABLE processed_images ADD COLUMN %s %s", col, typ)); err != nil {
				return fmt.Errorf("migrate add %s: %w", col, err)
			}
		}
	}
	// Rows that predate our tool are Python outputs; tag them so --migrate can
	// find them and so we never mistake them for our own canonical output.
	_, _ = s.db.ExecContext(ctx,
		"UPDATE processed_images SET tool = ? WHERE tool IS NULL OR tool = ''", ToolLegacy)
	return nil
}

func (s *Store) columns(ctx context.Context, table string) (map[string]bool, error) {
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf("PRAGMA table_info(%s)", table))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]bool{}
	for rows.Next() {
		var (
			cid     int
			name    string
			typ     string
			notnull int
			dflt    sql.NullString
			pk      int
		)
		if err := rows.Scan(&cid, &name, &typ, &notnull, &dflt, &pk); err != nil {
			return nil, err
		}
		out[name] = true
	}
	return out, rows.Err()
}

// ProcessedSet returns every recorded content hash (seed set for idempotency).
func (s *Store) ProcessedSet(ctx context.Context) (map[string]struct{}, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := s.db.QueryContext(ctx, "SELECT hash FROM processed_images")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	set := map[string]struct{}{}
	for rows.Next() {
		var h string
		if err := rows.Scan(&h); err != nil {
			return nil, err
		}
		set[h] = struct{}{}
	}
	return set, rows.Err()
}

// LegacyUnmigratedHashes returns hashes produced by the old Python tool that
// --migrate has not yet unified into the new canonical format.
func (s *Store) LegacyUnmigratedHashes(ctx context.Context) (map[string]struct{}, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := s.db.QueryContext(ctx,
		"SELECT hash FROM processed_images WHERE tool = ? AND IFNULL(migrated,0) = 0", ToolLegacy)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	set := map[string]struct{}{}
	for rows.Next() {
		var h string
		if err := rows.Scan(&h); err != nil {
			return nil, err
		}
		set[h] = struct{}{}
	}
	return set, rows.Err()
}

// IsProcessed returns the record for a content hash, if any.
func (s *Store) IsProcessed(ctx context.Context, hash string) (Record, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var r Record
	err := s.db.QueryRowContext(ctx, `
		SELECT hash, IFNULL(filename,''), IFNULL(media_kind,''), IFNULL(format,''),
		       IFNULL(tool,''), IFNULL(tool_version,''), IFNULL(migrated,0),
		       IFNULL(outcome,''), IFNULL(stamp,'')
		FROM processed_images WHERE hash = ? LIMIT 1`, hash).Scan(
		&r.Hash, &r.Filename, &r.Media, &r.Format, &r.Tool, &r.Version, &r.Migrated, &r.Outcome, &r.Stamp)
	if errors.Is(err, sql.ErrNoRows) {
		return Record{}, false, nil
	}
	if err != nil {
		return Record{}, false, err
	}
	return r, true, nil
}

// shouldMergePaths: markProcessed/MarkNoChange upsert and merge the "|" filename list.

// MarkCompressed records a hash we produced (canonical output on disk).
func (s *Store) MarkCompressed(ctx context.Context, hash, relPath, media, format, tool, version string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.upsert(ctx, s.db, hash, relPath, media, format, tool, version, OutcomeCompressed, "")
}

// MarkNoChange records that a source hash was evaluated but could not be shrunk
// under `stamp` settings, so future runs with the SAME stamp can skip it cheaply.
func (s *Store) MarkNoChange(ctx context.Context, hash, relPath, media, format, tool, version, stamp string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.upsert(ctx, s.db, hash, relPath, media, format, tool, version, OutcomeNoChange, stamp)
}

// MarkMigrated flips migrated=1 for a hash (used by --migrate, runs once).
func (s *Store) MarkMigrated(ctx context.Context, hash string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.ExecContext(ctx, "UPDATE processed_images SET migrated = 1 WHERE hash = ?", hash)
	return err
}

// BeginSwap journals an intended atomic replace BEFORE the rename happens.
func (s *Store) BeginSwap(ctx context.Context, sourceHash, expectedHash, finalPath, tmpPath string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	res, err := s.db.ExecContext(ctx,
		"INSERT INTO staged(source_hash, expected_hash, final_path, tmp_path, created_at) VALUES(?,?,?,?,?)",
		sourceHash, expectedHash, finalPath, tmpPath, time.Now().UTC().Format(time.RFC3339))
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

// CommitSwap finalizes the swap: records the new hash as processed and removes
// the staged row in one transaction.
func (s *Store) CommitSwap(ctx context.Context, stagedID int64, newHash, relPath, media, format, tool, version string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := s.upsert(ctx, tx, newHash, relPath, media, format, tool, version, OutcomeCompressed, ""); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM staged WHERE id = ?", stagedID); err != nil {
		return err
	}
	return tx.Commit()
}

// upsert inserts/updates a processed row, merging the "|" filename list. It
// works both on the DB and inside a transaction (both satisfy execer).
func (s *Store) upsert(ctx context.Context, ex execer, hash, relPath, media, format, tool, version, outcome, stamp string) error {
	var existing sql.NullString
	err := ex.QueryRowContext(ctx, "SELECT filename FROM processed_images WHERE hash = ?", hash).Scan(&existing)
	merged := relPath
	if err == nil && existing.Valid && existing.String != "" {
		set := map[string]struct{}{}
		for _, p := range strings.Split(existing.String, "|") {
			if p != "" {
				set[p] = struct{}{}
			}
		}
		set[relPath] = struct{}{}
		merged = strings.Join(sortedStable(keysOf(set)), "|")
	} else if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	_, err = ex.ExecContext(ctx, `
		INSERT INTO processed_images(hash, filename, media_kind, format, tool, tool_version, outcome, stamp, updated_at)
		VALUES(?,?,?,?,?,?,?,?,?)
		ON CONFLICT(hash) DO UPDATE SET
			filename=excluded.filename,
			media_kind=COALESCE(NULLIF(excluded.media_kind,''), processed_images.media_kind),
			format=COALESCE(NULLIF(excluded.format,''), processed_images.format),
			tool=excluded.tool,
			tool_version=excluded.tool_version,
			outcome=excluded.outcome,
			stamp=excluded.stamp,
			updated_at=excluded.updated_at`,
		hash, merged, media, format, tool, version, outcome, stamp, time.Now().UTC().Format(time.RFC3339))
	return err
}

type execer interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// PendingStaged returns interrupted swaps awaiting recovery.
func (s *Store) PendingStaged(ctx context.Context) ([]StagedRow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := s.db.QueryContext(ctx,
		"SELECT id, IFNULL(source_hash,''), expected_hash, final_path, tmp_path FROM staged")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []StagedRow
	for rows.Next() {
		var r StagedRow
		if err := rows.Scan(&r.ID, &r.SourceHash, &r.ExpectedHash, &r.FinalPath, &r.TmpPath); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// DropStaged deletes a staged row (used by recovery when a swap is discarded).
func (s *Store) DropStaged(ctx context.Context, id int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.ExecContext(ctx, "DELETE FROM staged WHERE id = ?", id)
	return err
}

func keysOf(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func sortedStable(in []string) []string {
	out := append([]string(nil), in...)
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j] < out[j-1]; j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}
