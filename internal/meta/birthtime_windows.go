//go:build windows

package meta

import (
	"golang.org/x/sys/windows"
)

// CopyBirthTime copies the NTFS creation time from src to dst. os.Chtimes cannot
// set creation time, so we call SetFileTime directly. Errors are returned but
// callers treat them as non-fatal (best-effort nicety).
func CopyBirthTime(src, dst string) error {
	ct, err := BirthTimeOf(src)
	if err != nil {
		return err
	}
	return SetBirthTime(dst, ct)
}

// BirthTimeOf reads the NTFS creation time of path (0 when unavailable).
// Capture it BEFORE any swap that may replace src itself.
func BirthTimeOf(path string) (int64, error) {
	return getCreateTime(path)
}

// SetBirthTime stamps a previously captured creation time (as returned by
// BirthTimeOf) onto dst.
func SetBirthTime(dst string, ct int64) error {
	if ct == 0 {
		return nil
	}
	// Reopen the creation time exactly as it was on the source.
	creation := windows.Filetime{LowDateTime: uint32(ct & 0xFFFFFFFF), HighDateTime: uint32(ct >> 32)}

	h, err := openWrite(dst)
	if err != nil {
		return err
	}
	defer windows.CloseHandle(h)
	return windows.SetFileTime(h, &creation, nil, nil)
}

func getCreateTime(path string) (int64, error) {
	h, err := openRead(path)
	if err != nil {
		return 0, err
	}
	defer windows.CloseHandle(h)
	var creation, lastAccess, lastWrite windows.Filetime
	if err := windows.GetFileTime(h, &creation, &lastAccess, &lastWrite); err != nil {
		return 0, err
	}
	return int64(uint64(uint32(creation.HighDateTime))<<32 | uint64(uint32(creation.LowDateTime))), nil
}

func openRead(path string) (windows.Handle, error) {
	p, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return 0, err
	}
	return windows.CreateFile(p, windows.FILE_READ_ATTRIBUTES,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil, windows.OPEN_EXISTING, windows.FILE_ATTRIBUTE_NORMAL, 0)
}

func openWrite(path string) (windows.Handle, error) {
	p, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return 0, err
	}
	return windows.CreateFile(p, windows.FILE_WRITE_ATTRIBUTES,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil, windows.OPEN_EXISTING, windows.FILE_ATTRIBUTE_NORMAL, 0)
}
