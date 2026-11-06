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
// An older database (even the Python-era one, table processed_images with just
// hash+filename) is adopted as-is: our columns are added non-destructively.
// A completed walk ends with the journal GC (PruneUnseen), which deletes rows
// for content that no longer exists anywhere in the archive.
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

	"github.com/Llloooggg/ImageCompressor/internal/safefs"
	_ "modernc.org/sqlite"
)

const driver = "sqlite"

// Outcome of a processed record.
const (
	OutcomeCompressed = "compressed" // we produced/own the canonical file on disk
	OutcomeNoChange   = "nochange"   // evaluated; could not shrink -> original kept
)

// Record is a processed file entry returned by IsProcessed.
type Record struct {
	Hash     string
	Filename string // "|"-joined relative paths seen for this content
	Media    string // image | video | animated (NOT derivable from the ext: avif is both photo and animated)
	Outcome  string // compressed | nochange
	Stamp    string // settings fingerprint for nochange decisions
	Achieved string // actual result quality ("svt p8 crf=34 vmaf=96.4", "avifenc s6 q=85"); "" = unknown
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
	db   *sql.DB
	path string // DB file path, so the WAL sidecar names are known for cleanup
	mu   sync.Mutex
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
	s := &Store{db: db, path: path}
	if err := s.migrate(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }

// Checkpoint folds everything committed in the WAL into the main DB file and
// truncates the WAL to zero. The connection pool is capped at one connection
// (see Open), so this cannot be BUSY against ourselves.
func (s *Store) Checkpoint(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var busy, logFrames, checkpointed int
	if err := s.db.QueryRowContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`).
		Scan(&busy, &logFrames, &checkpointed); err != nil {
		return fmt.Errorf("wal_checkpoint: %w", err)
	}
	if busy != 0 {
		return fmt.Errorf("wal_checkpoint: blocked with %d frames still in the log", logFrames)
	}
	return nil
}

// CloseClean ends the run's work with the DB: fold the WAL into the main file,
// close the connection, THEN remove the sidecars. The order is the whole
// point - deleting -wal/-shm while the database is open is exactly what
// corrupts a WAL database, so they are only ever dropped after Close.
//
// A run that finishes this way leaves one self-contained .db (the sidecars
// come back on the next open), so copying that single file is a valid backup
// at all times. After a hard kill the sidecars stay on purpose: a non-empty
// -wal holds committed journal rows, and SQLite replays them on the next open.
func (s *Store) CloseClean(ctx context.Context) error {
	if err := s.Checkpoint(ctx); err != nil {
		// not fatal: the WAL is still replayed on the next open, nothing is lost
		fmt.Fprintf(os.Stderr, "Warning: WAL checkpoint skipped: %v\n", err)
	}
	if err := s.db.Close(); err != nil {
		return err
	}
	safefs.RemoveIfExists(s.path + "-wal")
	safefs.RemoveIfExists(s.path + "-shm")
	return nil
}

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
		// seen is a re-read cache, NOT a second source of truth: it remembers
		// which content hash a path had under a specific (size, mtime) stat
		// triple. A repeat run over an unchanged archive can then skip hashing
		// gigabytes entirely. Any mismatch (edit, restore, touch) falls back to
		// the real hash - processed_images stays the only authority.
		`CREATE TABLE IF NOT EXISTS seen (
			path  TEXT PRIMARY KEY,
			size  INTEGER NOT NULL,
			mtime INTEGER NOT NULL,
			hash  TEXT NOT NULL
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
		"media_kind": "TEXT",
		"updated_at": "TEXT",
		"outcome":    "TEXT",
		"stamp":      "TEXT",
		"achieved":   "TEXT", // actual result quality: "svt p8 crf=34 vmaf=96.4" / "avifenc s6 q=85"
	}
	for col, typ := range want {
		if !have[col] {
			if _, err := s.db.ExecContext(ctx,
				fmt.Sprintf("ALTER TABLE processed_images ADD COLUMN %s %s", col, typ)); err != nil {
				return fmt.Errorf("migrate add %s: %w", col, err)
			}
		}
	}
	// The old columns (tool/tool_version/migrated/format) are gone from the
	// schema and from every query. If an existing DB still has them (pre-
	// cleanup), they simply stay untouched and unused: the one-off DB prep
	// script drops them; the code never references them either way.
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

// IsProcessed returns the record for a content hash, if any.
func (s *Store) IsProcessed(ctx context.Context, hash string) (Record, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var r Record
	err := s.db.QueryRowContext(ctx, `
		SELECT hash, IFNULL(filename,''), IFNULL(media_kind,''),
		       IFNULL(outcome,''), IFNULL(stamp,''), IFNULL(achieved,'')
		FROM processed_images WHERE hash = ? LIMIT 1`, hash).Scan(
		&r.Hash, &r.Filename, &r.Media, &r.Outcome, &r.Stamp, &r.Achieved)
	if errors.Is(err, sql.ErrNoRows) {
		return Record{}, false, nil
	}
	if err != nil {
		return Record{}, false, err
	}
	return r, true, nil
}

// SeenHash returns the content hash cached for path when the file's size and
// mtime still match what was hashed. A miss means the caller must read the
// file (new file, edited file, restored original, touched metadata). The
// cache is a convenience only: processed_images remains the source of truth,
// and every hit is still verified against it.
func (s *Store) SeenHash(ctx context.Context, path string, size, mtime int64) (string, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var h string
	err := s.db.QueryRowContext(ctx,
		`SELECT hash FROM seen WHERE path = ? AND size = ? AND mtime = ?`,
		path, size, mtime).Scan(&h)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return h, true, nil
}

// RememberPath caches path -> content hash under the stat triple that proves
// it was not modified since. Best effort: a failure is not worth stopping a
// run (the next run just hashes that file again).
func (s *Store) RememberPath(ctx context.Context, path string, size, mtime int64, hash string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO seen(path, size, mtime, hash) VALUES(?,?,?,?)
		ON CONFLICT(path) DO UPDATE SET
			size = excluded.size, mtime = excluded.mtime, hash = excluded.hash`,
		path, size, mtime, hash)
	return err
}

// RenamePath follows a rename (lying-extension fix, honest output name) so the
// cached triple does not stay attached to a path that no longer exists.
func (s *Store) RenamePath(ctx context.Context, oldPath, newPath string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.db.ExecContext(ctx, `UPDATE seen SET path = ? WHERE path = ?`, newPath, oldPath); err != nil {
		// The target already has a cache row (primary-key clash): the old key
		// is the stale one, drop it. Callers record the fresh triple right
		// after, so nothing of value is lost either way.
		_, derr := s.db.ExecContext(ctx, `DELETE FROM seen WHERE path = ?`, oldPath)
		if derr != nil {
			return derr
		}
		return nil
	}
	return nil
}

// shouldMergePaths: markProcessed/MarkNoChange upsert and merge the "|" filename list.

// MarkCompressed records a hash we produced (canonical output on disk).
// achieved carries the actual result quality ("svt p8 crf=34 vmaf=96.4").
func (s *Store) MarkCompressed(ctx context.Context, hash, relPath, media, achieved string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.upsert(ctx, s.db, hash, relPath, media, OutcomeCompressed, "", achieved)
}

// MarkNoChange records that a source hash was evaluated but could not be shrunk
// under `stamp` settings, so future runs with the SAME stamp can skip it cheaply.
func (s *Store) MarkNoChange(ctx context.Context, hash, relPath, media, stamp string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.upsert(ctx, s.db, hash, relPath, media, OutcomeNoChange, stamp, "")
}

// PruneUnseen deletes journal rows for content that a COMPLETED walk never
// observed. Those rows are history of files that no longer exist (externally
// deleted, replaced, renamed by other means, or an archive redo that restored
// originals over our outputs). Keeping them only bloats the DB and pollutes
// audits. This runs only after a full, successful, in-place (non-dry-run,
// non-stopped) walk.
//
// Safety guard: if `seen` contains fewer than half the hashes the DB already
// knows, we are almost certainly walking a SUBFOLDER against a shared --db,
// and pruning would eat records of files that simply weren't in scope. In that
// case we delete nothing and report guarded=true so the caller can warn.
func (s *Store) PruneUnseen(ctx context.Context, seen map[string]struct{}) (deleted int64, guarded bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var total int64
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM processed_images`).Scan(&total); err != nil {
		return 0, false, err
	}
	if total == 0 {
		return 0, false, nil
	}
	// Too few rows observed -> partial walk against a shared DB: refuse.
	if int64(len(seen))*2 < total {
		return 0, true, nil
	}

	// NOT IN (large list) is the expensive part; do it in SQL with a temp table
	// rather than string-building placeholders, and stay inside one tx so a
	// concurrent writer (single-writer anyway) can't slip in.
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, false, err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `CREATE TEMP TABLE _seen(hash TEXT PRIMARY KEY)`); err != nil {
		return 0, false, err
	}
	stmt, err := tx.PrepareContext(ctx, `INSERT INTO _seen(hash) VALUES(?)`)
	if err != nil {
		return 0, false, err
	}
	defer stmt.Close()
	for h := range seen {
		if _, err := stmt.ExecContext(ctx, h); err != nil {
			return 0, false, err
		}
	}
	res, err := tx.ExecContext(ctx, `DELETE FROM processed_images WHERE hash NOT IN (SELECT hash FROM _seen)`)
	if err != nil {
		return 0, false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, false, err
	}
	// Drop re-read cache entries whose content hash no longer exists: the path
	// they describe belongs to dead content (restored original, deleted file).
	if _, err := tx.ExecContext(ctx, `DELETE FROM seen WHERE hash NOT IN (SELECT hash FROM processed_images)`); err != nil {
		return 0, false, err
	}
	if _, err := tx.ExecContext(ctx, `DROP TABLE _seen`); err != nil {
		return 0, false, err
	}
	if err := tx.Commit(); err != nil {
		return 0, false, err
	}
	return n, false, nil
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
func (s *Store) CommitSwap(ctx context.Context, stagedID int64, newHash, relPath, media, achieved string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := s.upsert(ctx, tx, newHash, relPath, media, OutcomeCompressed, "", achieved); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM staged WHERE id = ?", stagedID); err != nil {
		return err
	}
	return tx.Commit()
}

// upsert inserts/updates a processed row, merging the "|" filename list. It
// works both on the DB and inside a transaction (both satisfy execer).
func (s *Store) upsert(ctx context.Context, ex execer, hash, relPath, media, outcome, stamp, achieved string) error {
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
		INSERT INTO processed_images(hash, filename, media_kind, outcome, stamp, achieved, updated_at)
		VALUES(?,?,?,?,?,?,?)
		ON CONFLICT(hash) DO UPDATE SET
			filename=excluded.filename,
			media_kind=COALESCE(NULLIF(excluded.media_kind,''), processed_images.media_kind),
			outcome=excluded.outcome,
			stamp=excluded.stamp,
			achieved=COALESCE(NULLIF(excluded.achieved,''), processed_images.achieved),
			updated_at=excluded.updated_at`,
		hash, merged, media, outcome, stamp, achieved, time.Now().UTC().Format(time.RFC3339))
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
