//go:build !unix

package safefs

// fsyncDir is a no-op on platforms without a portable directory fsync
// (Windows, Plan 9). The rename itself is still atomic.
func fsyncDir(string) error { return nil }
