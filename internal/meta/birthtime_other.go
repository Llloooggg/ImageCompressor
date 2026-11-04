//go:build !windows

package meta

// CopyBirthTime is a no-op off Windows: unix has no portable creation-time API.
func CopyBirthTime(string, string) error { return nil }

// BirthTimeOf has no portable equivalent; 0 means "nothing to restore".
func BirthTimeOf(string) (int64, error) { return 0, nil }

// SetBirthTime is a no-op off Windows.
func SetBirthTime(string, int64) error { return nil }
