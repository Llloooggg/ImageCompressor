// Package meta preserves metadata and file times, as the original tool did.
// EXIF/ICC/XMP are moved with exiftool; filesystem mtime/atime are restored to
// the originals. Privacy/GPS is never scrubbed.
package meta

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"time"
)

// ErrNoExiftool means the exiftool binary wasn't found.
var ErrNoExiftool = errors.New("exiftool not found")

// TransferTags copies all metadata tags from orig to dst using exiftool.
// exifToolPath may be "" -> returns ErrNoExiftool (caller decides to warn).
func TransferTags(ctx context.Context, exifToolPath, orig, dst string) error {
	if exifToolPath == "" {
		return ErrNoExiftool
	}
	cmd := exec.CommandContext(ctx, exifToolPath,
		"-overwrite_original",
		"-TagsFromFile", orig,
		"-all:all",
		dst,
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("exiftool: %w: %s", err, string(out))
	}
	return nil
}

// CopyFileTimes restores mtime/atime of src onto dst.
func CopyFileTimes(src, dst string) error {
	si, err := os.Stat(src)
	if err != nil {
		return err
	}
	return os.Chtimes(dst, atimeOf(si), si.ModTime())
}

// CopyFileTimesFromInfo is a helper when src was already stat'ed.
func CopyFileTimesFromInfo(info fs.FileInfo, dst string) error {
	return os.Chtimes(dst, atimeOf(info), info.ModTime())
}

func atimeOf(info fs.FileInfo) time.Time {
	if d, ok := info.(interface{ AccessTime() time.Time }); ok {
		return d.AccessTime()
	}
	return info.ModTime()
}
