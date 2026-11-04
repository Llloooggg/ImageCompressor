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
		{"auto full", EncAuto, full, []string{"av1_nvenc", "av1_amf", "av1_qsv", "libsvtav1"}},
		{"auto no svt falls back to libaom", EncAuto, noSvt, []string{"av1_nvenc", "libaom-av1"}},
		{"auto essentials no gpu", EncAuto, noGpuNoSvt, []string{"libaom-av1"}},
		{"auto probe failed keeps svt", EncAuto, map[string]bool{}, []string{"libsvtav1"}},
		{"cpu svt", EncCPU, full, []string{"libsvtav1"}},
		{"cpu no svt falls back to libaom", EncCPU, noSvt, []string{"libaom-av1"}},
		{"max always libaom", "max", full, []string{"libaom-av1"}},
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
