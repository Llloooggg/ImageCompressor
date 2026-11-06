// Command ImageCompressor compresses a family photo/video archive to canonical
// formats (photo/animated -> AVIF, video -> MP4+AV1) with metadata preserved,
// idempotently and crash-safely. Defaults are chosen so that running with no
// arguments compresses the executable's own folder recursively.
//
// All program-facing text (UI, logs, errors) is English on purpose: console
// codepages mangle other locales, and the changelog must stay greppable.
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
		fmt.Println("\nStopping: running files finish safely, no new files start. Press Ctrl+C again to exit immediately.")
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
		defer signal.Stop(ch)
		<-ch
		fmt.Println("Hard stop. Next run resumes where this one left off.")
		os.Exit(1)
	}()

	if err := runMain(ctx, cfg); err != nil {
		fmt.Fprintln(os.Stderr, "Error:", err)
		pauseIfInteractive()
		os.Exit(1)
	}
	pauseIfInteractive()
}

func runMain(ctx context.Context, cfg *config.Config) error {
	loc := tools.NewLocator()
	defer assets.Cleanup()
	warnTools(loc)

	st, err := store.Open(ctx, cfg.Database)
	if err != nil {
		return fmt.Errorf("open DB: %w", err)
	}
	defer st.Close()

	fmt.Printf("Input:  %s\n", cfg.Input)
	if !cfg.InPlace() {
		fmt.Printf("Output: %s\n", cfg.Output)
	} else {
		fmt.Printf("Mode:   in-place\n")
	}
	fmt.Printf("DB:     %s\n", cfg.Database)
	ladder := media.CRFLadder(cfg.VideoCRF)
	guardTag := ""
	if cfg.VideoVMAF > 0 {
		guardTag = fmt.Sprintf(", vmaf>=%.0f", cfg.VideoVMAF)
	}
	pl := media.PhotoLadder(cfg.PhotoQ)
	photoDesc := fmt.Sprintf("lossless q=%d", cfg.PhotoQ)
	if !cfg.PhotoLoss {
		pguard := ""
		if cfg.PhotoVMAF > 0 {
			pguard = fmt.Sprintf(", vmaf>=%.0f", cfg.PhotoVMAF)
		}
		photoDesc = fmt.Sprintf("auto-ladder q %d->%d%s", pl[0], pl[len(pl)-1], pguard)
	}
	fmt.Printf("Photo:  AVIF %s   Video: MP4+AV1 auto-ladder crf %d->%d%s, mode %s\n",
		photoDesc, ladder[0], ladder[len(ladder)-1], guardTag, cfg.VideoEnc)

	if !cfg.DryRun && !cfg.Yes {
		if !confirm(ctx) {
			fmt.Println("Cancelled.")
			return nil
		}
	}

	r := runner.New(cfg, st, loc)
	if err := r.Run(ctx); err != nil && ctx.Err() == nil {
		return err
	}
	fmt.Println(r.Summary())
	if ctx.Err() != nil {
		fmt.Println("\nStopped on request. Run the same command again to resume where it stopped.")
	}
	return nil
}

func warnTools(loc *tools.Locator) {
	need := map[string]string{
		"ffmpeg":   "video/AV1 encoding and AVIF fallback",
		"exiftool": "EXIF/ICC/XMP metadata transfer",
		"avifenc":  "best AVIF for photos, else ffmpeg fallback",
	}
	var missing []string
	for t, why := range need {
		if loc.Resolve(t) == "" {
			missing = append(missing, fmt.Sprintf("%s - %s", t, why))
		}
	}
	if len(missing) > 0 {
		fmt.Println("Warning, external tools not found:")
		for _, m := range missing {
			fmt.Println("  -", m)
		}
		fmt.Println("Put them in PATH or in a tools/ folder next to this exe.")
	}
}

func confirm(ctx context.Context) bool {
	fmt.Println("\nWarning: files will be modified in place. Continue? [y/N]: ")
	reader := bufio.NewReader(os.Stdin)
	line, err := reader.ReadString('\n')
	// Drop any leftover keystrokes queued during the prompt so they can't be
	// mistaken for the end-of-run pause's Enter later.
	flushStdin()
	if ctx.Err() != nil {
		return false
	}
	if err != nil && line == "" {
		return false
	}
	return len(line) > 0 && (line[0] == 'y' || line[0] == 'Y')
}

func pauseIfInteractive() {
	if isTerminal(os.Stdin) {
		// Any Enter/keys typed DURING the (possibly multi-hour) run sit unread
		// in the console buffer. Flush first, then demand a fresh Enter: this
		// guarantees the double-clicked window stays open until the user has
		// actually read the summary, instead of closing on a stray keystroke.
		flushStdin()
		fmt.Println("\nPress Enter to exit...")
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
