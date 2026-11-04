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
	Input     string
	Output    string  // empty == in-place (== Input)
	Database  string  // empty == <Input>/image_compressor.db
	PhotoQ    int     // AVIF quality 0..100
	PhotoLoss bool    // force pixel-exact lossless AVIF
	PhotoVMAF float64 // min VMAF for escalated photo q levels (0 disables the quality guard)
	VideoCRF  int     // AV1 CRF floor (0..50); the tool auto-escalates per file (+6, +12) when it can't shrink
	VideoVMAF float64 // min VMAF for escalated CRF levels (0 disables the quality guard)
	VideoEnc  string  // auto|gpu|cpu|nvenc|amf|qsv|libaom (see media)
	Workers   int
	Migrate   bool // one-shot unify of legacy-python outputs
	FixExt    bool // ONLY fix lying extensions (no compression); extensions are also always fixed during normal runs
	DryRun    bool
	Yes       bool // skip confirmation
	Verbose   bool
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

	fs.StringVar(&c.Input, "input", defInput, "папка с медиа (по умолчанию — папка исполняемого файла)")
	fs.StringVar(&c.Output, "output", "", "папка результата (пусто = сжимать на месте)")
	fs.StringVar(&c.Database, "db", "", "путь к БД (пусто = <input>/image_compressor.db)")
	fs.IntVar(&c.PhotoQ, "photo-q", 85, "качество AVIF для фото, 0..100 (выше = лучше/тяжелее); если фото не сжимается, инструмент сам понижает ступени -6/-12")
	fs.Float64Var(&c.PhotoVMAF, "photo-vmaf-min", 95, "минимальный VMAF для пониженных ступеней q для фото (0 = отключить проверку качества)")
	fs.BoolVar(&c.PhotoLoss, "photo-lossless", false, "AVIF без потерь (пиксель-в-пиксель, тяжелее)")
	fs.IntVar(&c.VideoCRF, "video-crf", 28, "базовый CRF для AV1 (0..50; меньше = лучше/тяжелее); при невозможности сжать инструмент сам поднимает его ступенями +6/+12")
	fs.Float64Var(&c.VideoVMAF, "video-vmaf-min", 95, "минимальный VMAF для повышенных ступеней CRF (0 = отключить проверку качества)")
	fs.IntVar(&c.Workers, "workers", defWorkers, fmt.Sprintf("число параллельных воркеров (по умолчанию %d)", defWorkers))
	fs.StringVar(&c.VideoEnc, "video-encoder", "auto", "код видео: auto|gpu|cpu|nvenc|amf|qsv|libaom (auto=GPU c фолбеком на CPU)")
	var gpu, cpu, max bool
	fs.BoolVar(&gpu, "gpu", false, "только аппаратный AV1 (gpu)")
	fs.BoolVar(&cpu, "cpu", false, "только SVT-AV1 (cpu)")
	fs.BoolVar(&max, "max", false, "максимальное сжатие libaom-av1, медленно (libaom)")
	fs.BoolVar(&c.Migrate, "migrate", false, "одноразово привести legacy-python выходы к новому канону")
	fs.BoolVar(&c.FixExt, "fix-ext", false, "отдельный режим: только починить врущие расширения без сжатия (обычный режим чинит их всегда)")
	fs.BoolVar(&c.DryRun, "dry-run", false, "показать, что будет сделано, не меняя файлы")
	fs.BoolVar(&c.Yes, "yes", false, "без подтверждения (для автоматизации)")
	fs.BoolVar(&c.Verbose, "v", false, "подробный лог")

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
