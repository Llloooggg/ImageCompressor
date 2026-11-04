package safefs

import (
	"os"
	"path/filepath"
	"testing"
)

func TestTempNameKeepsSuffix(t *testing.T) {
	dir := t.TempDir()
	final := filepath.Join(dir, "photo.avif")
	tmp, err := TempName(final)
	if err != nil {
		t.Fatal(err)
	}
	if filepath.Ext(tmp) != ".avif" {
		t.Fatalf("temp should keep .avif suffix, got %s", tmp)
	}
	if !IsTempName(tmp) {
		t.Fatal("IsTempName false for our temp")
	}
}

func TestAtomicReplaceOverwrites(t *testing.T) {
	dir := t.TempDir()
	final := filepath.Join(dir, "f.bin")
	if err := os.WriteFile(final, []byte("ORIGINAL"), 0o644); err != nil {
		t.Fatal(err)
	}
	tmp, _ := TempName(final)
	if err := os.WriteFile(tmp, []byte("NEW"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := AtomicReplace(tmp, final); err != nil {
		t.Fatal(err)
	}
	got, _ := os.ReadFile(final)
	if string(got) != "NEW" {
		t.Fatalf("want NEW, got %s", got)
	}
	if _, err := os.Stat(tmp); !os.IsNotExist(err) {
		t.Fatal("tmp should be gone after rename")
	}
}

func TestCleanupTemps(t *testing.T) {
	root := t.TempDir()
	sub := filepath.Join(root, "a")
	_ = os.MkdirAll(sub, 0o755)
	tmp := filepath.Join(sub, ".mc-tmp-deadbeef.jpg")
	keep := filepath.Join(sub, "keep.jpg")
	_ = os.WriteFile(tmp, []byte("x"), 0o644)
	_ = os.WriteFile(keep, []byte("y"), 0o644)
	n, err := CleanupTemps(root)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("removed %d want 1", n)
	}
	if _, err := os.Stat(keep); err != nil {
		t.Fatal("non-temp must survive cleanup")
	}
}
