package detect

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

func writeFile(t *testing.T, name string, b []byte) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(p, b, 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func avifBytes() []byte {
	// 24-byte ftyp box: size(BE) 'ftyp' major=avif minor=0 brands: avif,mif1
	out := make([]byte, 24)
	binary.BigEndian.PutUint32(out[0:4], 24)
	copy(out[4:8], "ftyp")
	copy(out[8:12], "avif")
	copy(out[12:16], []byte{0, 0, 0, 0})
	copy(out[16:20], "avif")
	copy(out[20:24], "mif1")
	return out
}

func TestDetectAVIFBrand(t *testing.T) {
	p := writeFile(t, "out.avif", avifBytes())
	res, err := Detect(p)
	if err != nil {
		t.Fatal(err)
	}
	if res.Format != FmtAVIF {
		t.Fatalf("want avif, got %s", res.Format)
	}
	if res.Kind != KindImage {
		t.Fatalf("want KindImage, got %s", res.Kind)
	}
}

func TestDetectAVIFLyingAsJPG(t *testing.T) {
	p := writeFile(t, "mislabelled.jpg", avifBytes())
	res, err := Detect(p)
	if err != nil {
		t.Fatal(err)
	}
	if res.Format != FmtAVIF {
		t.Fatalf("want avif regardless of .jpg ext, got %s", res.Format)
	}
	if !res.ExtensionLies {
		t.Fatal("expected ExtensionLies=true for avif named .jpg")
	}
}

func TestDetectJPEG(t *testing.T) {
	// SOI + APP0 JFIF marker enough for filetype.
	jpg := []byte{0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10, 'J', 'F', 'I', 'F', 0x00}
	p := writeFile(t, "a.jpg", jpg)
	res, err := Detect(p)
	if err != nil {
		t.Fatal(err)
	}
	if res.Format != FmtJPEG || res.Kind != KindImage {
		t.Fatalf("got %s/%s", res.Format, res.Kind)
	}
	if res.ExtensionLies {
		t.Fatal(".jpg honest for jpeg")
	}
}
