//go:build !windows

package vt

import "os"

// Enable is a no-op on unix-like terminals: callers only ask after a TTY
// check, and real unix TTYs speak VT100 natively.
func Enable(*os.File) bool { return true }
