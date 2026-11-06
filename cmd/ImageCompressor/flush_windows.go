//go:build windows

package main

import "syscall"

// flushStdin drops everything typed into the console but never read. During a
// multi-hour run nobody reads stdin, so stray keys/Enters pile up; without
// this flush the end-of-run "Press Enter to exit..." (and the y/N prompt)
// would instantly consume that backlog and the double-clicked window would
// close as if by itself -- exactly what the user must not lose the summary.
func flushStdin() {
	kernel32 := syscall.NewLazyDLL("kernel32.dll")
	flush := kernel32.NewProc("FlushConsoleInputBuffer")
	h, err := syscall.GetStdHandle(-10) // STD_INPUT_HANDLE
	if err != nil || h == 0 || h == ^syscall.Handle(0) {
		return // no console (piped/redirected): nothing to flush
	}
	flush.Call(uintptr(h))
}
