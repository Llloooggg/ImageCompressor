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

func TestDetectAVI(t *testing.T) {
	// RIFF <size> "AVI LIST" is the whole magic filetype needs.
	avi := make([]byte, 16)
	copy(avi[0:4], "RIFF")
	binary.LittleEndian.PutUint32(avi[4:8], 1<<20)
	copy(avi[8:16], "AVI LIST")
	p := writeFile(t, "clip.avi", avi)
	res, err := Detect(p)
	if err != nil {
		t.Fatal(err)
	}
	if res.Format != FmtAVI || res.Kind != KindVideo {
		t.Fatalf("got %s/%s, want avi/video", res.Format, res.Kind)
	}
	if res.ExtensionLies {
		t.Fatal(".avi honest for avi")
	}
	if FmtAVI.HonestExt() != ".avi" {
		t.Fatalf("honest ext = %q", FmtAVI.HonestExt())
	}
}

// isoFile builds a minimal 'ftyp' box with the given major brand and
// compatible brands — enough for both filetype and our brand refiner.
func isoFile(major string, brands ...string) []byte {
	brands = append([]string{major}, brands...)
	size := 24 + 4*len(brands)
	out := make([]byte, size)
	binary.BigEndian.PutUint32(out[0:4], uint32(size))
	copy(out[4:8], "ftyp")
	copy(out[8:12], major)
	at := 16
	for _, b := range brands {
		copy(out[at:at+4], b)
		at += 4
	}
	return out
}

// .mp4/.mov/.m4v are ONE honest family for ISO-BMFF video. Naming a
// QuickTime-brand clip .mp4 is not a lie: the live archive ping-ponged six
// files between .mp4 and .mov on every run because the honest-extension fixer
// said .mov while the encoder target said .mp4.
func TestISOFamilySuffixesAreAllHonest(t *testing.T) {
	qt := isoFile("qt  ", "qt  ", "mp42", "isom")
	for _, name := range []string{"clip.mov", "clip.mp4", "clip.m4v", "clip.MOV"} {
		p := writeFile(t, name, qt)
		res, err := Detect(p)
		if err != nil {
			t.Fatal(err)
		}
		if res.Kind != KindVideo {
			t.Fatalf("%s: kind = %s, want video", name, res.Kind)
		}
		if res.ExtensionLies {
			t.Fatalf("%s: iso-family suffix must not count as a lie (format=%s)", name, res.Format)
		}
	}

	// Same family the other way round: isom bytes under a .mov name.
	p := writeFile(t, "clip.mov", isoFile("isom", "isom", "iso2", "mp41"))
	res, err := Detect(p)
	if err != nil {
		t.Fatal(err)
	}
	if res.Format != FmtMP4 {
		t.Fatalf("brand refiner: got %s, want mp4", res.Format)
	}
	if res.ExtensionLies {
		t.Fatal("isom bytes under .mov are still iso-family: honest")
	}

	// The family rule must not smear across OTHER containers: an AVI or MKV
	// name on iso bytes is still a lie and still gets fixed.
	for _, name := range []string{"clip.avi", "clip.mkv", "clip.webm", "clip.jpg"} {
		p := writeFile(t, name, qt)
		res, err := Detect(p)
		if err != nil {
			t.Fatal(err)
		}
		if !res.ExtensionLies {
			t.Fatalf("%s: %s on iso bytes must still be reported as a lie", name, res.Format)
		}
	}
}
