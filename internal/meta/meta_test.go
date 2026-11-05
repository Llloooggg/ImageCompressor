package meta

import (
	"bytes"
	"context"
	"image"
	"image/color"
	"image/png"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Llloooggg/ImageCompressor/internal/tools"
)

// TestTransferTagsUnicodeNames reproduces the battle-archive failure: a file
// named "пук пук ... 🚨😈 [4757027433021].mp4" made exiftool die with
// "File '...' does not exist for -tagsFromFile" because argv passes through
// the OEM code page and non-ASCII turns into garbage. The fix feeds the
// paths through a UTF-8 stdin argument file with -charset filename=UTF8.
func TestTransferTagsUnicodeNames(t *testing.T) {
	et := tools.NewLocator().Exiftool()
	if et == "" {
		t.Skip("no exiftool (bundled or PATH), integration test skipped")
	}
	ctx := context.Background()
	dir := t.TempDir()

	// minimal valid PNG
	var buf bytes.Buffer
	img := image.NewRGBA(image.Rect(0, 0, 8, 8))
	for y := range 8 {
		for x := range 8 {
			img.Set(x, y, color.RGBA{uint8(x * 30), uint8(y * 30), 128, 255})
		}
	}
	if err := png.Encode(&buf, img); err != nil {
		t.Fatal(err)
	}
	pngBytes := buf.Bytes()

	// seed tags while the name is still pure ASCII (tag VALUES are ASCII:
	// the test is about the FILE NAMES, writing Unicode values via argv is a
	// different exiftool concern we deliberately do not exercise here)
	seed := filepath.Join(dir, "seed.png")
	if err := os.WriteFile(seed, pngBytes, 0o644); err != nil {
		t.Fatal(err)
	}
	//nolint:gosec // et is a resolved tool path, args are fixed
	if out, err := exec.Command(et, "-overwrite_original", "-Artist=Tagged By Test", "-Description=from test", seed).CombinedOutput(); err != nil {
		t.Fatalf("seed tags: %v: %s", err, out)
	}

	// the killer name: Cyrillic + spaces + emoji + square brackets,
	// exactly the shape of the file that failed in the real archive
	src := filepath.Join(dir, "пук пук я пыталась сделать ахегао 🚨😈 [4757027433021].png")
	dst := filepath.Join(dir, "копия выходного дня 🚨 [4757027433022].png")
	if err := os.Rename(seed, src); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dst, pngBytes, 0o644); err != nil {
		t.Fatal(err)
	}

	if err := TransferTags(ctx, et, src, dst); err != nil {
		t.Fatalf("TransferTags on unicode names: %v", err)
	}

	got := exifRead(t, et, dst, "-Artist", "-Description")
	for _, want := range []string{"Tagged By Test", "from test"} {
		if !strings.Contains(got, want) {
			t.Errorf("tag %q missing after transfer, dump:\n%s", want, got)
		}
	}
}

// exifRead reads tags back through the same unicode-safe argfile route.
func exifRead(t *testing.T, et, file string, tags ...string) string {
	t.Helper()
	cmd := exec.Command(et, "-charset", "filename=UTF8", "-@", "-")
	cmd.Stdin = strings.NewReader(strings.Join(tags, "\n") + "\n" + file + "\n")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("read tags: %v: %s", err, out)
	}
	return string(out)
}
