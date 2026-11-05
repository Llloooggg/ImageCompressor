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

func TestChangeLogHeaderRestatedOnOldFormat(t *testing.T) {
	p := filepath.Join(t.TempDir(), "old.log")
	old := "Date\tAction\tSource\tResult\tSrcBytes\tDstBytes\tSavedBytes\tKind\tFormat\tQuality\tComment\n"
	if err := os.WriteFile(p, []byte("\ufeff"+old), 0o644); err != nil {
		t.Fatal(err)
	}
	cl, err := openChangeLog(p)
	if err != nil {
		t.Fatal(err)
	}
	cl.record("error", "", "", 0, 0, 0, "", "", "", "boom")
	cl.close()

	b, _ := os.ReadFile(p)
	text := string(b)
	if !strings.Contains(text, old+changeLogHeader+"\n") {
		t.Fatalf("new header must be restated right above new rows, got %q", text)
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
