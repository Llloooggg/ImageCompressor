// Package config holds CLI options and their safe defaults. Defaults are chosen
// so that running the executable with no arguments compresses its own folder
// recursively (double-click UX). Flags remain available for overrides.
package config

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
)

type Config struct {
	Input        string
	Output       string  // empty == in-place (== Input)
	Database     string  // empty == <Input>/image_compressor.db
	PhotoQ       int     // AVIF quality 0..100
	PhotoLoss    bool    // force pixel-exact lossless AVIF
	PhotoSpeed   int     // avifenc speed 0..10, higher = faster/bigger (default 6 = best-size canon)
	PhotoVMAF    float64 // min VMAF for escalated photo q levels (0 disables the quality guard)
	VideoCRF     int     // AV1 CRF floor (0..50); the tool auto-escalates per file (+6, +12) when it can't shrink
	VideoVMAF    float64 // min VMAF for escalated CRF levels (0 disables the quality guard)
	VideoEnc     string  // auto|gpu|cpu|nvenc|amf|qsv|libaom (see media)
	Workers      int
	VideoWorkers int  // concurrent heavy video encodes (lane count); 0 = auto
	VideoPreset  int  // SVT-AV1 preset 2..12 (0 = codec default); higher = faster, bigger
	Still        bool // collapse still-image videos (one static frame + soundtrack) into a photo; default on
	Migrate      bool // one-shot unify of legacy-python outputs
	FixExt       bool // ONLY fix lying extensions (no compression); extensions are also always fixed during normal runs
	DryRun       bool
	Yes          bool // skip confirmation
	Verbose      bool
}

// ExecDir returns the directory containing the running executable.
func ExecDir() string {
	exe, err := os.Executable()
	if err != nil {
		return "."
	}
	if resolved, err := filepath.EvalSymlinks(exe); err == nil {
		exe = resolved
	}
	return filepath.Dir(exe)
}

// DefaultWorkers avoids oversubscribing internally-multithreaded encoders.
func DefaultWorkers() int {
	c := runtime.NumCPU()
	w := c / 2
	if w < 1 {
		w = 1
	}
	if w > 16 {
		w = 16
	}
	return w
}

// Parse builds a Config from CLI args. Default input = executable's directory.
func Parse(args []string) (*Config, error) {
	fs := flag.NewFlagSet("image_compressor", flag.ContinueOnError)
	c := &Config{}
	defInput := ExecDir()
	defWorkers := DefaultWorkers()

	fs.StringVar(&c.Input, "input", defInput, "media folder (default: the executable's own folder)")
	fs.StringVar(&c.Output, "output", "", "result folder (empty = compress in place)")
	fs.StringVar(&c.Database, "db", "", "database path (empty = <input>/image_compressor.db)")
	fs.IntVar(&c.PhotoQ, "photo-q", 85, "AVIF quality for photos, 0..100; if a photo cannot shrink, the tool lowers q by -6/-12 steps on its own")
	fs.Float64Var(&c.PhotoVMAF, "photo-vmaf-min", 95, "min VMAF for lowered photo steps, vs original (0 = disable quality guard)")
	fs.BoolVar(&c.PhotoLoss, "photo-lossless", false, "lossless AVIF, pixel-exact, larger")
	fs.IntVar(&c.PhotoSpeed, "photo-speed", 6, "avifenc speed 0..10: higher = faster, slightly bigger files; 6 = best size (the canon, keep unless desperate)")
	fs.IntVar(&c.VideoCRF, "video-crf", 28, "base AV1 CRF 0..50, lower = better/heavier; the tool raises it by +6/+12 steps when it cannot shrink")
	fs.Float64Var(&c.VideoVMAF, "video-vmaf-min", 95, "min VMAF for raised CRF steps, vs original (0 = disable quality guard)")
	fs.IntVar(&c.Workers, "workers", defWorkers, fmt.Sprintf("parallel workers (default %d)", defWorkers))
	fs.IntVar(&c.VideoWorkers, "video-workers", 0, "videos encoded side by side; each gets cores/video-workers threads (default auto: cores/8, so one SVT-AV1 can actually unfold)")
	fs.IntVar(&c.VideoPreset, "video-preset", 8, "SVT-AV1 preset 2..12: lower = smarter/smaller/slower, 8 = balanced default; only affects CPU SVT")
	var noStill bool
	fs.BoolVar(&noStill, "no-still", false, "keep still-image videos, one static frame with a soundtrack glued on, as videos; by default such files collapse into one photo frame and the soundtrack is dropped")
	fs.StringVar(&c.VideoEnc, "video-encoder", "auto", "video encoders: auto|gpu|cpu|nvenc|amf|qsv|libaom (auto = CPU only, SVT-AV1; hardware encoders are opt-in)")
	var gpu, cpu, max bool
	fs.BoolVar(&gpu, "gpu", false, "hardware AV1 only, mode gpu")
	fs.BoolVar(&cpu, "cpu", false, "SVT-AV1 CPU only, mode cpu")
	fs.BoolVar(&max, "max", false, "smallest output via libaom-av1, slow, mode libaom")
	fs.BoolVar(&c.Migrate, "migrate", false, "one-shot: convert legacy-python outputs to the new canon; keep the flag until it reports nothing left to migrate")
	fs.BoolVar(&c.FixExt, "fix-ext", false, "separate mode: only fix lying extensions, no compression; normal mode always fixes them too")
	fs.BoolVar(&c.DryRun, "dry-run", false, "show what would happen without touching files")
	fs.BoolVar(&c.Yes, "yes", false, "skip the confirmation prompt, for automation")
	fs.BoolVar(&c.Verbose, "v", false, "verbose per-file logging")

	if err := fs.Parse(args); err != nil {
		return nil, err
	}

	// Booleans override --video-encoder for convenience; precedence max>gpu>cpu.
	switch {
	case max:
		c.VideoEnc = "libaom"
	case gpu:
		c.VideoEnc = "gpu"
	case cpu:
		c.VideoEnc = "cpu"
	}
	c.Still = !noStill
	if c.VideoEnc == "" {
		c.VideoEnc = "auto"
	}

	if c.Input == "" {
		c.Input = defInput
	}
	abs, err := filepath.Abs(c.Input)
	if err != nil {
		return nil, err
	}
	c.Input = abs

	if c.Output != "" {
		o, err := filepath.Abs(c.Output)
		if err != nil {
			return nil, err
		}
		c.Output = o
	}

	if c.Database == "" {
		c.Database = filepath.Join(c.Input, "image_compressor.db")
	}
	return c, nil
}

// InPlace reports whether we operate directly on Input (no separate output dir).
func (c *Config) InPlace() bool {
	return c.Output == "" || c.Output == c.Input
}
