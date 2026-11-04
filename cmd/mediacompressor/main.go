// Command mediacompressor compresses a family photo/video archive to canonical
// formats (photo/animated -> AVIF, video -> MP4+AV1) with metadata preserved,
// idempotently and crash-safely. Defaults are chosen so that running with no
// arguments compresses the executable's own folder recursively.
//
//go:generate go run github.com/josephspurrier/goversioninfo/cmd/goversioninfo@v1.7.0 -64
package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/Llloooggg/ImageCompressor/internal/assets"
	"github.com/Llloooggg/ImageCompressor/internal/config"
	"github.com/Llloooggg/ImageCompressor/internal/media"
	"github.com/Llloooggg/ImageCompressor/internal/runner"
	"github.com/Llloooggg/ImageCompressor/internal/store"
	"github.com/Llloooggg/ImageCompressor/internal/tools"
)

func main() {
	cfg, err := config.Parse(os.Args[1:])
	if err != nil {
		// flag package already printed usage / handled -h.
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	// Ctrl+C = graceful stop (current temps cleaned, next run resumes).
	// Second Ctrl+C = immediate exit — the staged journal + atomic renames mean
	// even a hard kill cannot corrupt anything.
	go func() {
		<-ctx.Done()
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
		defer signal.Stop(ch)
		<-ch
		fmt.Println("\nЖёсткая остановка (данные в безопасности — следующий запуск всё восстановит).")
		os.Exit(1)
	}()

	if err := runMain(ctx, cfg); err != nil {
		fmt.Fprintln(os.Stderr, "Ошибка:", err)
		pauseIfInteractive()
		os.Exit(1)
	}
	pauseIfInteractive()
}

func runMain(ctx context.Context, cfg *config.Config) error {
	if cfg.Migrate && !cfg.InPlace() {
		return fmt.Errorf("--migrate работает только при сжатии на месте (без --output)")
	}

	loc := tools.NewLocator()
	defer assets.Cleanup()
	warnTools(loc)

	st, err := store.Open(ctx, cfg.Database)
	if err != nil {
		return fmt.Errorf("открытие БД: %w", err)
	}
	defer st.Close()

	fmt.Printf("Вход:  %s\n", cfg.Input)
	if !cfg.InPlace() {
		fmt.Printf("Выход: %s\n", cfg.Output)
	} else {
		fmt.Printf("Режим: на месте\n")
	}
	fmt.Printf("БД:    %s\n", cfg.Database)
	ladder := media.CRFLadder(cfg.VideoCRF)
	guardTag := ""
	if cfg.VideoVMAF > 0 {
		guardTag = fmt.Sprintf(", vmaf>=%.0f", cfg.VideoVMAF)
	}
	fmt.Printf("Фото:  AVIF (q=%d%s)   Видео: MP4+AV1 (crf %d→%d авто-лестница%s, %s)\n",
		cfg.PhotoQ, losslessTag(cfg.PhotoLoss), ladder[0], ladder[len(ladder)-1], guardTag, cfg.VideoEnc)

	if !cfg.DryRun && !cfg.Yes {
		if !confirm(ctx) {
			fmt.Println("Отменено.")
			return nil
		}
	}

	r := runner.New(cfg, st, loc)
	if err := r.Run(ctx); err != nil && ctx.Err() == nil {
		return err
	}
	fmt.Println(r.Summary())
	if ctx.Err() != nil {
		fmt.Println("\nОстановлено по запросу. Повторный запуск той же команды продолжит с того же места.")
	}
	return nil
}

func warnTools(loc *tools.Locator) {
	need := map[string]string{
		"ffmpeg":   "видео/AV1 и AVIF-fallback",
		"exiftool": "перенос метаданных (EXIF/ICC/XMP)",
		"avifenc":  "оптимальный AVIF для фото (иначе ffmpeg-fallback)",
	}
	var missing []string
	for t, why := range need {
		if loc.Resolve(t) == "" {
			missing = append(missing, fmt.Sprintf("%s (%s)", t, why))
		}
	}
	if len(missing) > 0 {
		fmt.Println("Внимание, не найдены внешние утилиты:")
		for _, m := range missing {
			fmt.Println("  -", m)
		}
		fmt.Println("(положи их в PATH или в папку tools/ рядом с этим exe)")
	}
}

func confirm(ctx context.Context) bool {
	fmt.Println("\nВнимание: файлы будут изменены на месте. Продолжить? [y/N]: ")
	reader := bufio.NewReader(os.Stdin)
	line, err := reader.ReadString('\n')
	if ctx.Err() != nil {
		return false
	}
	if err != nil && line == "" {
		return false
	}
	return len(line) > 0 && (line[0] == 'y' || line[0] == 'Y')
}

func losslessTag(l bool) string {
	if l {
		return ", lossless"
	}
	return ""
}

func pauseIfInteractive() {
	if isTerminal(os.Stdin) {
		fmt.Println("\nНажмите Enter для выхода...")
		bufio.NewReader(os.Stdin).ReadString('\n')
	}
}

func isTerminal(f *os.File) bool {
	fi, err := f.Stat()
	if err != nil {
		return false
	}
	return fi.Mode()&os.ModeCharDevice != 0
}
