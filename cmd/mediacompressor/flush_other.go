//go:build !windows

package main

// flushStdin: POSIX terminals deliver typed keys to the reader directly and
// there is no window lifecycle to protect; no-op keeps the call sites uniform.
func flushStdin() {}
