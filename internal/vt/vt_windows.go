//go:build windows

package vt

import (
	"os"
	"syscall"
	"unsafe"
)

var (
	kernel32           = syscall.NewLazyDLL("kernel32.dll")
	procGetConsoleMode = kernel32.NewProc("GetConsoleMode")
	procSetConsoleMode = kernel32.NewProc("SetConsoleMode")
)

const enableVirtualTerminalProcessing = 0x0004

// Enable turns on VT/ANSI sequence processing for the console behind f and
// reports whether the console will actually honour escape sequences.
func Enable(f *os.File) bool {
	h := syscall.Handle(f.Fd())
	var mode uint32
	if r, _, _ := procGetConsoleMode.Call(uintptr(h), uintptr(unsafe.Pointer(&mode))); r == 0 {
		return false // not a real console handle (redirected, pipe)
	}
	if mode&enableVirtualTerminalProcessing != 0 {
		return true // already on
	}
	if r, _, _ := procSetConsoleMode.Call(uintptr(h), uintptr(mode|enableVirtualTerminalProcessing)); r == 0 {
		return false
	}
	return true
}
