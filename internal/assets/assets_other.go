//go:build !windows

package assets

// Extract is a no-op off Windows: the helper binaries (exiftool needs a Perl
// module tree; avifenc needs its DLLs) are embedded only for the Windows build.
// On Linux/macOS the tool is expected to resolve ffmpeg/avifenc/exiftool from
// the system (e.g. package-manager installs).
func Extract() string { return "" }

// Cleanup is a no-op where nothing was extracted.
func Cleanup() {}
