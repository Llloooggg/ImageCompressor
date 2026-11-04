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
