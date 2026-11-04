//go:build unix

package safefs

import (
	"errors"
	"os"
	"syscall"
)

// fsyncDir flushes directory entries so the rename survives a power loss.
// On unix the directory handle is fsync-able. EINVAL/EACCES can happen on some
// pseudo-filesystems; we tolerate those rather than failing the whole swap.
func fsyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		if errors.Is(err, syscall.EINVAL) || errors.Is(err, syscall.EACCES) || errors.Is(err, syscall.EPERM) {
			return nil
		}
		return err
	}
	return nil
}
