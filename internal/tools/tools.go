// Package tools resolves external codecs/binaries at runtime, mirroring the old
// Python model of shipping tools/ next to the exe. Search order: a tools/ dir
// next to the executable (and cwd), then the system PATH.
package tools

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
)

// Locator caches resolved tool paths.
type Locator struct {
	exeDir string
	cwdDir string
	mu     sync.Mutex
	cache  map[string]string
	miss   map[string]bool
}

func NewLocator() *Locator {
	return &Locator{
		exeDir: execDir(),
		cwdDir: func() string { wd, _ := os.Getwd(); return wd }(),
		cache: map[string]string{},
		miss:  map[string]bool{},
	}
}

// Resolve returns an absolute path to a tool, or "" if not found. It prefers a
// bundled copy under a tools/ dir, then falls back to PATH.
func (l *Locator) Resolve(name string) string {
	l.mu.Lock()
	defer l.mu.Unlock()
	if p, ok := l.cache[name]; ok {
		return p
	}
	if l.miss[name] {
		return ""
	}
	if exe := exeName(name); exe != name {
		if p := l.findBundled(exe); p != "" {
			l.cache[name] = p
			return p
		}
	}
	if p, err := exec.LookPath(name); err == nil {
		l.cache[name] = p
		return p
	}
	if p, err := exec.LookPath(exeName(name)); err == nil {
		l.cache[name] = p
		return p
	}
	l.miss[name] = true
	return ""
}

func (l *Locator) findBundled(exe string) string {
	bases := []string{
		filepath.Join(l.exeDir, "tools"),
		filepath.Join(l.exeDir, "bin"),
		l.exeDir,
	}
	if l.cwdDir != "" && l.cwdDir != l.exeDir {
		bases = append(bases, filepath.Join(l.cwdDir, "tools"), filepath.Join(l.cwdDir, "bin"))
	}
	for _, b := range bases {
		p := filepath.Join(b, exe)
		if st, err := os.Stat(p); err == nil && !st.IsDir() {
			return p
		}
	}
	return ""
}

// exeName appends .exe on Windows.
func exeName(name string) string {
	if runtime.GOOS == "windows" {
		if filepath.Ext(name) == ".exe" {
			return name
		}
		return name + ".exe"
	}
	return name
}

// FFmpeg/FFprobe conveniences.
func (l *Locator) FFmpeg() string  { return l.Resolve("ffmpeg") }
func (l *Locator) FFprobe() string { return l.Resolve("ffprobe") }
func (l *Locator) Exiftool() string { return l.Resolve("exiftool") }
func (l *Locator) Avifenc() string  { return l.Resolve("avifenc") }

// Run executes a tool with args and captures combined output.
func (l *Locator) Run(ctx context.Context, toolPath string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, toolPath, args...)
	out, err := cmd.CombinedOutput()
	return out, err
}

func execDir() string {
	exe, err := os.Executable()
	if err != nil {
		return "."
	}
	if r, err := filepath.EvalSymlinks(exe); err == nil {
		exe = r
	}
	return filepath.Dir(exe)
}
