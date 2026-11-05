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
	// Unicode file names (Cyrillic/emoji) get mangled on Windows before
	// exiftool's Perl layer ever sees them: command-line arguments are
	// transcoded through the OEM/ANSI code page and non-ASCII turns into
	// '?' ("File ... does not exist for -tagsFromFile"). The documented
	// Unicode-safe route is a UTF-8 ARGUMENT FILE plus
	// "-charset filename=UTF8". We feed it through stdin ("-@ -"): no temp
	// file, no extra paths in argv at all. In argument files exiftool takes
	// one raw argument per line and does NOT process quotes or escapes, so
	// the paths go verbatim (file names cannot contain newlines on any OS
	// we target, so line separation is unambiguous).
	cmd := exec.CommandContext(ctx, exifToolPath,
		"-overwrite_original",
		"-charset", "filename=UTF8",
		"-@", "-",
	)
	in, err := cmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("exiftool stdin: %w", err)
	}
	go func() {
		fmt.Fprintf(in, "-TagsFromFile\n%s\n-all:all\n%s\n", orig, dst)
		in.Close()
	}()
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
