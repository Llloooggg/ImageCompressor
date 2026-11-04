//go:build !windows

package meta

// CopyBirthTime is a no-op off Windows: unix has no portable creation-time API.
func CopyBirthTime(string, string) error { return nil }
