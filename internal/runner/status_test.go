package runner

import (
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Llloooggg/ImageCompressor/internal/config"
)

// captureStdout swaps os.Stdout for a pipe while f runs and returns what was
// printed (printStatus writes via fmt.Print -> os.Stdout).
func captureStdout(t *testing.T, f func()) string {
	t.Helper()
	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = w
	done := make(chan string)
	go func() {
		b, _ := io.ReadAll(r)
		done <- string(b)
	}()
	f()
	w.Close()
	os.Stdout = old
	return <-done
}

func TestPrintStatusFixedBlock(t *testing.T) {
	r := &Runner{total: 3, multi: true, cfg: &config.Config{}}
	r.cfg.Workers = 2
	r.firstDraw.Store(true)
	r.done.Store(1)
	r.skipDone.Store(2) // two files were "already" done

	out1 := captureStdout(t, func() { r.printStatus(time.Now().Add(-time.Minute)) })
	if !strings.Contains(out1, "already:2") {
		t.Errorf("missing already: counter, got %q", out1)
	}
	if strings.Contains(out1, " done:2") {
		t.Errorf("old done: label still present: %q", out1)
	}
	if !strings.Contains(out1, "(33%)") {
		t.Errorf("expected truncated 33%% for 1/3, got %q", out1)
	}
	// fixed height: header + 2 idle worker slots + last row, all on first draw
	if got := strings.Count(out1, "\n"); got != 3 {
		t.Errorf("block must be 4 rows (3 newlines), got %d: %q", got, out1)
	}
	if !strings.Contains(out1, "--") {
		t.Errorf("idle worker slots must show dashes: %q", out1)
	}
	if !strings.Contains(out1, "last") {
		t.Errorf("missing last-done row: %q", out1)
	}
	if strings.Contains(out1, "\x1b[") {
		t.Errorf("first draw must not move the cursor or clear: %q", out1)
	}

	out2 := captureStdout(t, func() { r.printStatus(time.Now()) })
	if !strings.Contains(out2, "\x1b[3A") {
		t.Errorf("second repaint must step up blockLines-1 rows, got %q", out2)
	}
	if got := strings.Count(out2, "\x1b[2K"); got != 4 {
		t.Errorf("repaint must clear every block row (%d), got %d: %q", 4, got, out2)
	}

	// the battle bug: 46422/46423 rounded to "100%" while workers still ran
	r2 := &Runner{total: 46423, multi: true, cfg: &config.Config{Workers: 1}}
	r2.firstDraw.Store(true)
	r2.done.Store(46422)
	out := captureStdout(t, func() { r2.printStatus(time.Now()) })
	if !strings.Contains(out, "(99%)") || strings.Contains(out, "(100%)") {
		t.Errorf("must show 99%% until every file is truly done, got %q", out)
	}
}

func TestStatusBlockSlots(t *testing.T) {
	r := &Runner{multi: true, processRoot: `C:\arc`, cfg: &config.Config{}}
	r.cfg.Workers = 2
	r.trackStart(`C:\arc\family\holiday.mp4`)
	r.trackStart(`C:\arc\family\second very long file name with a lot of characters here.mp4`)
	lines := r.statusBlock("hdr", 200)
	if len(lines) != 4 {
		t.Fatalf("want header + 2 slots + last row, got %d: %q", len(lines), lines)
	}
	if !strings.Contains(lines[1], `family\holiday.mp4`) {
		t.Errorf("slot 1 must show the relative path: %q", lines[1])
	}
	// time comes FIRST (fixed column), then the path
	if idxT, idxP := strings.Index(lines[1], "0s"), strings.Index(lines[1], `family\`); idxT == -1 || idxP == -1 || idxT > idxP {
		t.Errorf("elapsed time must precede the path: %q", lines[1])
	}

	r.trackEnd(`C:\arc\family\holiday.mp4`, 1200*time.Millisecond)
	lines = r.statusBlock("hdr", 200)
	if strings.Contains(lines[1]+lines[2], "holiday.mp4") {
		t.Errorf("finished file still in a worker slot: %q", lines)
	}
	last := lines[3]
	if !strings.Contains(last, "1.2s") || !strings.Contains(last, `family\holiday.mp4`) {
		t.Errorf("last row must show duration + relative path: %q", last)
	}
	// a slot row may never exceed the console width: long paths get clipped
	// (clip's ellipsis counted: statusBlock budgets maxw-3 for it)
	lines = r.statusBlock("hdr", 40)
	for _, ln := range lines {
		if len(ln) > 40 {
			t.Errorf("row exceeds width (%d): %q", len(ln), ln)
		}
	}
}
