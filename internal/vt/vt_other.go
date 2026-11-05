//go:build !windows

package vt

import "os"

// Enable is a no-op on unix-like terminals: callers only ask after a TTY
// check, and real unix TTYs speak VT100 natively.
func Enable(*os.File) bool { return true }

// Width returns 0 (unknown): the runner falls back to a conservative width so
// a live block never wraps. Getting the exact unix width would need an ioctl;
// the fallback is good enough for the secondary unix build.
func Width(*os.File) int { return 0 }
