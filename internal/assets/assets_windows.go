//go:build windows

// Package assets embeds the codec helper binaries (avifenc, exiftool and their
// dependency DLLs / Perl module tree) directly into the executable, mirroring
// the original project's PyInstaller "--add-binary tools/*.exe:tools" rule: the
// shipped .exe is self-contained for these helpers. At runtime we materialize the
// embedded tree to a temp dir (analogous to sys._MEIPASS) preserving structure,
// so exiftool.exe finds exiftool_files/ and avifenc.exe finds its DLLs beside it.
package assets

import (
	"embed"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
)

// all: keeps the whole subtree (including nested exiftool_files/ and DLLs).
//
//go:embed all:tools/windows
var windowsFS embed.FS

var (
	once sync.Once
	dir  string
)

// Extract materializes the embedded tools to a temp dir once and returns it,
// or "" if embedding is unavailable/failed.
func Extract() string {
	once.Do(func() {
		d, err := materialize(windowsFS, "tools/windows")
		if err == nil {
			dir = d
		}
	})
	return dir
}

func materialize(f fs.FS, sub string) (string, error) {
	entries, err := fs.ReadDir(f, sub)
	if err != nil || len(entries) == 0 {
		return "", err
	}
	base, err := os.MkdirTemp("", "mc-tools-*")
	if err != nil {
		return "", err
	}
	err = fs.WalkDir(f, sub, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, err := filepath.Rel(sub, p)
		if err != nil {
			return err
		}
		data, err := fs.ReadFile(f, p)
		if err != nil {
			return err
		}
		// skip the placeholder README
		if rel == "README.txt" {
			return nil
		}
		out := filepath.Join(base, rel)
		if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
			return err
		}
		return os.WriteFile(out, data, 0o755)
	})
	if err != nil {
		os.RemoveAll(base)
		return "", err
	}
	return base, nil
}

// Cleanup removes a previously extracted tools dir (best-effort, on exit).
func Cleanup() {
	if dir != "" {
		_ = os.RemoveAll(dir)
	}
}
