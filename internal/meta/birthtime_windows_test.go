//go:build windows

package meta

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/sys/windows"
)

func TestBirthTimeRoundTrip(t *testing.T) {
	f := filepath.Join(t.TempDir(), "x.bin")
	if err := os.WriteFile(f, []byte("hi"), 0o644); err != nil {
		t.Fatal(err)
	}
	want := windows.NsecToFiletime(time.Date(2001, 2, 3, 4, 5, 6, 0, time.Local).UnixNano())
	ct := int64(uint64(want.HighDateTime)<<32 | uint64(want.LowDateTime))

	if err := SetBirthTime(f, ct); err != nil {
		t.Fatal(err)
	}
	got, err := BirthTimeOf(f)
	if err != nil {
		t.Fatal(err)
	}
	if got != ct {
		t.Fatalf("birth time round trip: got %d want %d", got, ct)
	}
}
