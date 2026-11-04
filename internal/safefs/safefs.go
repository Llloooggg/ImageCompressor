// Package safefs provides crash-safe file operations. The guiding rule for a
// sensitive media archive: a crash at any point must never leave a half-written
// file in place of the original, and never make us re-compress already-compressed
// output. Everything is written to a sibling temp file, fsynced, then swapped in
// with an atomic rename, after which the directory is fsynced (on unix).
package safefs

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// tempPrefix marks every temp file we create. Cleanup on resume keys on it.
const tempPrefix = ".mc-tmp-"

// IsTempName reports whether a filename is one of our temp files.
func IsTempName(name string) bool {
	return strings.HasPrefix(filepath.Base(name), tempPrefix)
}

// TempName returns a unique temp path in the same directory as final, keeping
// the final suffix so the encoder muxes the right container while it writes.
func TempName(final string) (string, error) {
	dir := filepath.Dir(final)
	ext := filepath.Ext(final)
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	name := fmt.Sprintf("%s%s%s", tempPrefix, hex.EncodeToString(b[:]), ext)
	return filepath.Join(dir, name), nil
}

// SyncFile flushes a file's data+metadata to disk. Call on the temp file after
// an external encoder writes it and before the atomic rename.
//
// It reopens read-write on purpose: on Windows FlushFileBuffers (used by
// File.Sync) fails with ERROR_ACCESS_DENIED when the handle lacks write
// access, so a read-only open would break durability there.
func SyncFile(path string) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		// fall back to read-only (works on unix) if read-write isn't possible
		f, err = os.Open(path)
		if err != nil {
			return err
		}
	}
	defer f.Close()
	return f.Sync()
}

// AtomicReplace moves tmpPath onto finalPath atomically and durably.
// On Windows os.Rename uses MOVEFILE_REPLACE_EXISTING and overwrites the target.
// On unix the rename is atomic within the same directory.
func AtomicReplace(tmpPath, finalPath string) error {
	if err := os.Rename(tmpPath, finalPath); err != nil {
		return err
	}
	return fsyncDir(filepath.Dir(finalPath))
}

// RemoveIfExists removes a file, ignoring not-exist. Used to clean temp files
// after failures without masking the real error.
func RemoveIfExists(path string) {
	_ = os.Remove(path)
}

// CleanupTemps deletes leftover temp files under root from a previous crashed
// run. Returns how many were removed.
func CleanupTemps(root string) (int, error) {
	removed := 0
	err := filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			// Don't abort the whole scan on a single unreadable entry.
			if os.IsPermission(err) {
				return nil
			}
			return err
		}
		if !info.IsDir() && IsTempName(p) {
			if err := os.Remove(p); err == nil {
				removed++
			}
		}
		return nil
	})
	return removed, err
}

// CopyFileBytes copies src to dst fully and durably (fsync dst). Used when
// writing into a separate --output dir.
func CopyFileBytes(src, dst string) error {
	tmp, err := TempName(dst)
	if err != nil {
		return err
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		RemoveIfExists(tmp)
		return err
	}
	if err := out.Sync(); err != nil {
		out.Close()
		RemoveIfExists(tmp)
		return err
	}
	if err := out.Close(); err != nil {
		RemoveIfExists(tmp)
		return err
	}
	return AtomicReplace(tmp, dst)
}
