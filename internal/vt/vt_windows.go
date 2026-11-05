//go:build windows

package vt

import (
	"os"
	"syscall"
	"unsafe"
)

var (
	kernel32                   = syscall.NewLazyDLL("kernel32.dll")
	procGetConsoleMode         = kernel32.NewProc("GetConsoleMode")
	procSetConsoleMode         = kernel32.NewProc("SetConsoleMode")
	procGetConsoleScreenBuffer = kernel32.NewProc("GetConsoleScreenBufferInfo")
)

const enableVirtualTerminalProcessing = 0x0004

// csbi mirrors CONSOLE_SCREEN_BUFFER_INFO (only the fields before srWindow
// matter here; the layout up to srWindow is what GetConsoleScreenBufferInfo
// fills and we read the window rect from it).
type csbi struct {
	sizeX, sizeY          int16
	curX, curY            int16
	attrs                 uint16
	left, top, right, bot int16
	maxX, maxY            int16
}

// Width returns the console width in columns, or 0 if f is not a console.
func Width(f *os.File) int {
	var info csbi
	if r, _, _ := procGetConsoleScreenBuffer.Call(uintptr(syscall.Handle(f.Fd())), uintptr(unsafe.Pointer(&info))); r == 0 {
		return 0
	}
	w := int(info.right-info.left) + 1
	if w < 1 {
		return 0
	}
	return w
}

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
