package runner

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestChangeLogMBAndSeconds(t *testing.T) {
	p := filepath.Join(t.TempDir(), "image_compressor.log")
	cl, err := openChangeLog(p)
	if err != nil {
		t.Fatal(err)
	}
	cl.record("compressed", "in\\a.mp4", "in\\a.mp4", 5_000_000, 3_145_728, 12.3, "video", "mp4", "crf=34 vmaf=97.2", "")
	cl.close()

	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimLeft(string(b), "\ufeff"), "\n")
	if len(lines) < 2 {
		t.Fatalf("expected header + row, got %q", string(b))
	}
	if !strings.HasPrefix(lines[0], changeLogHeader) {
		t.Fatalf("header = %q", lines[0])
	}
	cols := strings.Split(lines[1], "\t")
	// Date Action Source Result SrcMB DstMB SavedMB Seconds Kind Format Quality Comment
	if len(cols) != 12 {
		t.Fatalf("got %d columns, want 12: %q", len(cols), lines[1])
	}
	if cols[1] != "compressed" || cols[2] != `in\a.mp4` {
		t.Errorf("cols = %q", cols)
	}
	want := []string{"4.768", "3.000", "1.768", "12.3"} // 5e6 B, 3 MiB, saved, secs
	for i, w := range want {
		if cols[4+i] != w {
			t.Errorf("col %d = %q, want %q (all: %q)", 4+i, cols[4+i], w, cols)
		}
	}
}

// Every new run on an existing log starts with a blank separator + a fresh
// header, so run boundaries are visible while scrolling (and the header always
// matches the columns of the rows under it, old byte-size rows included).
func TestChangeLogHeaderPerRun(t *testing.T) {
	p := filepath.Join(t.TempDir(), "old.log")
	old := "\ufeffDate\tAction\tSource\tResult\tSrcBytes\tDstBytes\tSavedBytes\tKind\tFormat\tQuality\tComment\n2026-01-01 00:00:00\tcompressed\told.mp4\told.mp4\t28538\t27200\t1338\tvideo\tmp4\tcrf=34\t\n"
	if err := os.WriteFile(p, []byte(old), 0o644); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		cl, err := openChangeLog(p)
		if err != nil {
			t.Fatal(err)
		}
		cl.record("error", "", "", 0, 0, 0, "", "", "", "boom")
		cl.close()
	}

	b, _ := os.ReadFile(p)
	text := string(b)
	if got := strings.Count(text, changeLogHeader+"\n"); got != 2 {
		t.Fatalf("each run restates the header once: got %d, want 2, log=%q", got, text)
	}
	if !strings.Contains(text, "\n\n"+changeLogHeader) {
		t.Errorf("blank separator must precede each fresh header: %q", text)
	}
	if !strings.Contains(text, old) {
		t.Errorf("previous run's rows must be preserved: %q", text)
	}
}

// a brand-new file gets BOM + header exactly once (no leading separator).
func TestChangeLogFreshFile(t *testing.T) {
	p := filepath.Join(t.TempDir(), "new.log")
	cl, err := openChangeLog(p)
	if err != nil {
		t.Fatal(err)
	}
	cl.record("error", "", "", 0, 0, 0, "", "", "", "boom")
	cl.close()

	b, _ := os.ReadFile(p)
	if !strings.HasPrefix(string(b), "\ufeff"+changeLogHeader+"\n") {
		t.Errorf("fresh log must start with BOM+header: %q", string(b))
	}
	if strings.HasPrefix(string(b), "\ufeff\n") {
		t.Errorf("fresh log must not start with a blank separator: %q", string(b))
	}
}

func TestClip(t *testing.T) {
	if got := clip("abc", 5); got != "abc" {
		t.Errorf("short string changed: %q", got)
	}
	// "привет мир" in UTF-8: byte-cutting must not split a rune
	long := "привет мир"
	got := clip(long, 7) // 7 bytes: lands mid-rune at byte 7
	if !strings.HasSuffix(got, "...") {
		t.Fatalf("no ellipsis: %q", got)
	}
	if !utf8.ValidString(strings.TrimSuffix(got, "...")) {
		t.Errorf("clip split a rune: %q", got)
	}
}
