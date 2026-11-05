package runner

import (
	"io"
	"os"
	"strings"
	"testing"
	"time"
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

func TestPrintStatusTwoLineVT(t *testing.T) {
	r := &Runner{total: 3, multi: true}
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
	if !strings.Contains(out1, "now: --") {
		t.Errorf("missing active line: %q", out1)
	}
	if strings.Contains(out1, "\x1b[1A") {
		t.Errorf("first repaint must not move the cursor up: %q", out1)
	}

	out2 := captureStdout(t, func() { r.printStatus(time.Now()) })
	if !strings.Contains(out2, "\x1b[1A\x1b[2K\r") {
		t.Errorf("second repaint must go up one line and clear, got %q", out2)
	}

	// the battle bug: 46422/46423 rounded to "100%" while workers still ran
	r2 := &Runner{total: 46423, multi: true}
	r2.firstDraw.Store(true)
	r2.done.Store(46422)
	out := captureStdout(t, func() { r2.printStatus(time.Now()) })
	if !strings.Contains(out, "(99%)") || strings.Contains(out, "(100%)") {
		t.Errorf("must show 99%% until every file is truly done, got %q", out)
	}
}

func TestActiveLineNames(t *testing.T) {
	r := &Runner{multi: true, processRoot: `C:\arc`}
	r.trackStart(`C:\arc\family\holiday.mp4`)
	r.trackStart(`C:\arc\family\second very long file name with a lot of characters here.mp4`)
	line := r.activeLine()
	if !strings.Contains(line, "holiday.mp4 0s") {
		t.Errorf("active file not listed: %q", line)
	}
	r.trackEnd(`C:\arc\family\holiday.mp4`, 1200*time.Millisecond)
	line = r.activeLine()
	nowPart, lastPart, found := strings.Cut(line, "  |  last:")
	if !found {
		t.Fatalf("last-done slot missing: %q", line)
	}
	if strings.Contains(nowPart, "holiday.mp4") {
		t.Errorf("finished file still in active list: %q", line)
	}
	if !strings.Contains(lastPart, "holiday.mp4 1.2s") {
		t.Errorf("last-done slot wrong: %q", line)
	}
	if len(line) > 110 {
		t.Errorf("line exceeds repaint width (%d): %q", len(line), line)
	}
}
