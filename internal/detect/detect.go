// Package detect determines a file's real type from its magic bytes — never
// from its extension. The legacy pipeline once saved WebP bytes under a .jpg
// name and several viewers choked on the lie; on the input side we always look
// at the bytes so the encoder can't be misled, and on the output side we always
// write the real format under an honest extension.
package detect

import (
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/h2non/filetype"
	"github.com/h2non/filetype/types"
)

// Kind is the coarse routing class.
type Kind int

const (
	KindUnknown Kind = iota
	KindImage
	KindVideo
	KindAnimated // animated GIF / animated WebP -> animated AVIF path
)

func (k Kind) String() string {
	switch k {
	case KindImage:
		return "image"
	case KindVideo:
		return "video"
	case KindAnimated:
		return "animated"
	default:
		return "unknown"
	}
}

// Format is a lowercase, stable name for the actual container.
type Format string

const (
	FmtUnknown  Format = "unknown"
	FmtJPEG     Format = "jpeg"
	FmtPNG      Format = "png"
	FmtWebP     Format = "webp"
	FmtGIF      Format = "gif"
	FmtAVIF     Format = "avif"
	FmtHEIC     Format = "heic"
	FmtBMP      Format = "bmp"
	FmtTIFF     Format = "tiff"
	FmtMP4      Format = "mp4"
	FmtMOV      Format = "mov"
	FmtMatroska Format = "mkv"
	FmtWebM     Format = "webm"
	FmtAVI      Format = "avi"
)

var extFor = map[Format]string{
	FmtJPEG:     ".jpg",
	FmtPNG:      ".png",
	FmtWebP:     ".webp",
	FmtGIF:      ".gif",
	FmtAVIF:     ".avif",
	FmtHEIC:     ".heic",
	FmtBMP:      ".bmp",
	FmtTIFF:     ".tif",
	FmtMP4:      ".mp4",
	FmtMOV:      ".mov",
	FmtMatroska: ".mkv",
	FmtWebM:     ".webm",
	FmtAVI:      ".avi",
}

// HonestExt returns the extension that matches the real format.
func (f Format) HonestExt() string {
	if e, ok := extFor[f]; ok {
		return e
	}
	return ""
}

// IsMediaExt reports whether ext (".jpg", "jpg", any case) is one of the known
// media extensions. Used to strip extension CHAINS like ".mp4.mov" or the
// doubled ".mp4.mp4" down to the real base name.
func IsMediaExt(ext string) bool {
	ext = strings.ToLower(ext)
	if ext == ".m4v" { // common alias of the MP4 container
		return true
	}
	for _, e := range extFor {
		if e == ext {
			return true
		}
	}
	return false
}

// videoFormats are the containers we treat as video.
var videoFormats = map[Format]bool{FmtMP4: true, FmtMOV: true, FmtMatroska: true, FmtWebM: true, FmtAVI: true}

// imageFormats are raster stills we can feed to an image encoder.
var imageFormats = map[Format]bool{FmtJPEG: true, FmtPNG: true, FmtWebP: true, FmtBMP: true, FmtTIFF: true, FmtHEIC: true}

// Result bundles everything the runner needs about one file.
type Result struct {
	Kind     Kind
	Format   Format
	Animated bool
	// ExtensionLies is true when the file's extension doesn't match its bytes.
	ExtensionLies bool
}

// Detect reads the header and classifies the file by content.
func Detect(path string) (Result, error) {
	f, err := os.Open(path)
	if err != nil {
		return Result{}, err
	}
	defer f.Close()

	t, err := filetype.MatchReader(f)
	if err != nil {
		return Result{}, err
	}

	format := formatFromType(t)

	// ISO Base Media (MP4/AVIF/HEIC) are all 'ftyp' boxes; the brand is the
	// authoritative signal and filetype is unreliable for AVIF/HEIC. Refine.
	if bf, ok := detectISOBrand(path); ok {
		if format == FmtUnknown || format == FmtMP4 {
			format = bf
		}
	}

	res := Result{Format: format}

	switch {
	case imageFormats[format]:
		res.Kind = KindImage
	case videoFormats[format]:
		res.Kind = KindVideo
	case format == FmtGIF:
		res.Kind = KindAnimated
		res.Animated = isAnimatedGIF(path)
	case format == FmtWebP:
		// WebP can be animated or a still; classify accordingly.
		if isAnimatedWebP(path) {
			res.Kind = KindAnimated
			res.Animated = true
		} else {
			res.Kind = KindImage
		}
	case format == FmtAVIF:
		// Could be a still, an animated image, or an AV1 video-in-AVIF.
		res.Kind = KindImage
	default:
		res.Kind = KindUnknown
	}

	res.ExtensionLies = extensionLies(path, format)
	return res, nil
}

// detectISOBrand parses an ISO Base Media 'ftyp' box and maps its brand to a
// container format. filetype is unreliable for AVIF/HEIC, so this is the
// authoritative classifier for those. Returns ok=false for non-ISO files.
func detectISOBrand(path string) (Format, bool) {
	f, err := os.Open(path)
	if err != nil {
		return FmtUnknown, false
	}
	defer f.Close()
	hdr := make([]byte, 12)
	if _, err := io.ReadFull(f, hdr); err != nil {
		return FmtUnknown, false
	}
	// box size (4 BE) then 'ftyp'
	if string(hdr[4:8]) != "ftyp" {
		return FmtUnknown, false
	}
	major := string(hdr[8:12])
	// compatible brands follow the 8-byte major+minor header
	body := make([]byte, 256)
	n, _ := f.ReadAt(body, 12)
	body = body[:n]

	has := func(b string) bool {
		for i := 0; i+4 <= len(body); i += 4 {
			if string(body[i:i+4]) == b {
				return true
			}
		}
		return false
	}

	switch {
	case major == "avif" || major == "avis" || has("avif") || has("avis"):
		return FmtAVIF, true
	case major == "heic" || major == "heix" || major == "heim" || major == "heis" ||
		has("heic") || has("heix") || has("mif1"):
		return FmtHEIC, true
	case major == "M4V " || major == "mmp4" || major == "avc1" ||
		has("isom") || has("iso2") || has("mp41") || has("mp42") || has("avc1") || has("M4V "):
		return FmtMP4, true
	}
	return FmtUnknown, false
}

func formatFromType(t types.Type) Format {
	switch t.Extension {
	case "jpg":
		return FmtJPEG
	case "png":
		return FmtPNG
	case "webp":
		return FmtWebP
	case "gif":
		return FmtGIF
	case "avif":
		return FmtAVIF
	case "heic":
		return FmtHEIC
	case "bmp":
		return FmtBMP
	case "tif":
		return FmtTIFF
	case "mp4":
		return FmtMP4
	case "mov":
		return FmtMOV
	case "mkv":
		return FmtMatroska
	case "webm":
		return FmtWebM
	case "avi":
		return FmtAVI
	default:
		return FmtUnknown
	}
}

// isoFamilyExts are the interchangeable suffixes of one container family: the
// ISO Base Media File Format. QuickTime-brand bytes under a .mp4 name (or
// isom-brand bytes under .mov) are NOT a lie — the container is the same
// family, every viewer opens both, and every video we write is a real .mp4.
//
// Without this rule the two naming paths contradicted each other on such
// files: ensureHonestExt renamed them to .mov (format's honest suffix) while
// the encoder target was .mp4, so the same binary ping-ponged the name back
// and forth on every run (six files in the live archive's other\ folder,
// logged on 11-05 and again on 11-06).
var isoFamilyExts = map[string]bool{".mp4": true, ".mov": true, ".m4v": true}

// extensionLies compares on-disk suffix with the honest extension for the real
// format. Missing suffix isn't a lie (we add one), only a wrong one is.
func extensionLies(path string, format Format) bool {
	want := format.HonestExt()
	if want == "" {
		return false
	}
	have := strings.ToLower(filepath.Ext(path))
	if have == "" {
		return false
	}
	// .jpeg and .jpg are both honest for jpeg.
	if format == FmtJPEG && have == ".jpeg" {
		return false
	}
	// .mp4/.mov/.m4v are one honest family for ISO-BMFF video.
	if (format == FmtMP4 || format == FmtMOV) && isoFamilyExts[have] {
		return false
	}
	return have != want
}

// isAnimatedGIF checks for a NETSCAPE2.x loop extension or multiple image
// descriptors. Cheap: it only needs a small window.
func isAnimatedGIF(path string) bool {
	data, err := os.ReadFile(path)
	if err != nil || len(data) < 13 {
		return false
	}
	// Count image descriptors (0x2C separators) after the header.
	frames := 0
	for i := 13; i < len(data); i++ {
		if data[i] == 0x2C {
			frames++
			if frames > 1 {
				return true
			}
		}
		if data[i] == 0x21 && i+3 < len(data) && data[i+1] == 0xF9 {
			// graphic control extension present; still need 2nd frame to be animated
		}
	}
	return frames > 1
}

// isAnimatedWebP looks for the ANIM/ANMF chunk introduced by the VP8X extended
// header.
func isAnimatedWebP(path string) bool {
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer f.Close()
	buf := make([]byte, 12)
	if _, err := f.Read(buf); err != nil {
		return false
	}
	// RIFF....WEBP
	if string(buf[0:4]) != "RIFF" || string(buf[8:12]) != "WEBP" {
		return false
	}
	chunk := make([]byte, 8)
	for {
		if _, err := f.Read(chunk); err != nil {
			return false
		}
		id := string(chunk[0:4])
		if id == "ANIM" || id == "ANMF" {
			return true
		}
		size := binary.LittleEndian.Uint32(chunk[4:8])
		// chunks are padded to even size
		if _, err := f.Seek(int64(size+(size&1)), 1); err != nil {
			return false
		}
	}
}
