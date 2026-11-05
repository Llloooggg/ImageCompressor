package media

import (
	"reflect"
	"testing"
)

func TestBuildVideoChain(t *testing.T) {
	full := map[string]bool{"av1_nvenc": true, "av1_amf": true, "av1_qsv": true, "libsvtav1": true, "libaom-av1": true}
	// gyan "essentials": no SVT-AV1, GPU AV1 present.
	noSvt := map[string]bool{"av1_nvenc": true, "libaom-av1": true}
	// GPU-less machine with an essentials build (embedded ffmpeg case).
	noGpuNoSvt := map[string]bool{"libaom-av1": true}

	tests := []struct {
		name  string
		mode  string
		avail map[string]bool
		want  []string
	}{
		{"auto is cpu-only canon", EncAuto, full, []string{"libsvtav1"}},
		{"auto no svt falls back to libaom", EncAuto, noSvt, []string{"libaom-av1"}},
		{"auto essentials no gpu", EncAuto, noGpuNoSvt, []string{"libaom-av1"}},
		{"auto probe failed keeps svt", EncAuto, map[string]bool{}, []string{"libsvtav1"}},
		{"cpu svt", EncCPU, full, []string{"libsvtav1"}},
		{"cpu no svt falls back to libaom", EncCPU, noSvt, []string{"libaom-av1"}},
		{"max always libaom", "max", full, []string{"libaom-av1"}},
		{"gpu orders nvenc amf qsv", EncGPU, full, []string{"av1_nvenc", "av1_amf", "av1_qsv"}},
		{"gpu only", EncGPU, noGpuNoSvt, []string{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := Opts{VideoCRF: 24, Encoder: tt.mode}
			got := o.BuildVideoChain(tt.avail).Codecs()
			if len(got) == 0 && len(tt.want) == 0 {
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("chain = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCodecShort(t *testing.T) {
	cases := map[string]string{
		"libsvtav1":  "svt",
		"libaom-av1": "aom",
		"av1_nvenc":  "nvenc",
		"av1_amf":    "amf",
		"av1_qsv":    "qsv",
		"ffmpeg":     "aom",
		"avifenc":    "avifenc",
		"weird":      "weird",
	}
	for in, want := range cases {
		if got := codecShort(in); got != want {
			t.Errorf("codecShort(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestCRFLadder(t *testing.T) {
	tests := []struct {
		base int
		want []int
	}{
		{24, []int{24, 30, 36}},
		{28, []int{28, 34, 40}},
		{36, []int{36, 42, 45}}, // capped at 45, no duplicates
		{40, []int{40, 45}},
		{45, []int{45}},
		{2, []int{2, 8, 14}},
	}
	for _, tt := range tests {
		if got := CRFLadder(tt.base); !reflect.DeepEqual(got, tt.want) {
			t.Errorf("CRFLadder(%d) = %v, want %v", tt.base, got, tt.want)
		}
	}
}

// PhotoLadder is the stills mirror of CRFLadder: q steps DOWN (-6, -12),
// floored at 61, no duplicates.
func TestPhotoLadder(t *testing.T) {
	tests := []struct {
		base int
		want []int
	}{
		{85, []int{85, 79, 73}},
		{100, []int{100, 94, 88}},
		{70, []int{70, 64, 61}}, // floored at 61, no duplicates
		{66, []int{66, 61}},
		{61, []int{61}},
		{50, []int{61}}, // base below floor
	}
	for _, tt := range tests {
		if got := PhotoLadder(tt.base); !reflect.DeepEqual(got, tt.want) {
			t.Errorf("PhotoLadder(%d) = %v, want %v", tt.base, got, tt.want)
		}
	}
}

// CPU encoder calls must carry the per-child thread slice; GPU encoders must not.
func TestVideoFFArgsThreads(t *testing.T) {
	svt := cand{"libsvtav1", "-crf", "28"}
	aom := cand{"libaom-av1", "-crf", "28"}
	nv := cand{"av1_nvenc", "-cq", "28"}

	has := func(args []string, pair ...string) bool {
		for i := 0; i+1 < len(args); i++ {
			if args[i] == pair[0] && args[i+1] == pair[1] {
				return true
			}
		}
		return false
	}
	if a := videoFFArgs(svt, []string{"-c:a", "copy"}, 4, false, "s", "t"); !has(a, "-svtav1-params", "lp=4") {
		t.Errorf("svt call missing lp budget: %v", a)
	}
	if a := videoFFArgs(aom, []string{"-c:a", "copy"}, 4, false, "s", "t"); !has(a, "-threads", "4") {
		t.Errorf("libaom call missing thread cap: %v", a)
	}
	if a := videoFFArgs(nv, []string{"-c:a", "copy"}, 4, false, "s", "t"); has(a, "-threads", "4") || has(a, "-svtav1-params", "lp=4") {
		t.Errorf("nvenc call must not carry CPU thread flags: %v", a)
	}
	if a := videoFFArgs(svt, []string{"-c:a", "copy"}, 0, false, "s", "t"); has(a, "-svtav1-params", "lp=0") {
		t.Errorf("threads=0 must leave codec default: %v", a)
	}
	// normalize adds the honest bt709 color conversion (gbr-tagged screen
	// recordings make AV1 encoders refuse to init: ffmpeg -22).
	a := videoFFArgs(svt, []string{"-c:a", "copy"}, 4, true, "s", "t")
	if !has(a, "-vf", "scale=out_color_matrix=bt709") || !has(a, "-colorspace", "bt709") {
		t.Errorf("normalize pass missing bt709 conversion: %v", a)
	}
	if a := videoFFArgs(svt, []string{"-c:a", "copy"}, 4, false, "s", "t"); has(a, "-vf") {
		t.Errorf("normal pass must not touch color: %v", a)
	}
}

func TestWithCRFRebuild(t *testing.T) {
	avail := map[string]bool{"av1_nvenc": true, "libsvtav1": true}
	ch := Opts{VideoCRF: 28, Encoder: EncAuto}.BuildVideoChain(avail)
	up := ch.WithCRF(34)

	// Same encoder order, every quality value rebuilt at the new level.
	if !reflect.DeepEqual(ch.Codecs(), up.Codecs()) {
		t.Fatalf("order changed: %v -> %v", ch.Codecs(), up.Codecs())
	}
	for _, c := range up.cands {
		if c.value != "34" && c.codec != "av1_nvenc" {
			t.Errorf("%s value = %q, want rebuilt at 34", c.codec, c.value)
		}
	}
	if up.cands[0].codec == "av1_nvenc" && up.cands[0].value != "34" {
		t.Errorf("nvenc cq = %q, want 34", up.cands[0].value)
	}
	// Base chain must not be mutated; rebuild at the same level is a no-op.
	if ch.crf != 28 || ch.cands[0].value != "28" {
		t.Fatalf("base chain mutated: crf=%d value=%q", ch.crf, ch.cands[0].value)
	}
	if same := up.WithCRF(34); same.crf != 34 {
		t.Errorf("WithCRF(34) on 34-chain changed: %d", same.crf)
	}
	// Mode/availability survive the rebuild (no re-probe).
	if up.mode != EncAuto || !up.avail["libsvtav1"] {
		t.Errorf("mode/avail lost: mode=%q avail=%v", up.mode, up.avail)
	}
}
