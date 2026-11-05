package runner

import (
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Llloooggg/ImageCompressor/internal/config"
	"github.com/Llloooggg/ImageCompressor/internal/detect"
	"github.com/Llloooggg/ImageCompressor/internal/media"
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
	// fixed height: header + 2 idle worker slots, all on first draw
	if got := strings.Count(out1, "\n"); got != 2 {
		t.Errorf("block must be 3 rows (2 newlines), got %d: %q", got, out1)
	}
	if !strings.Contains(out1, "--") {
		t.Errorf("idle worker slots must show dashes: %q", out1)
	}
	if strings.Contains(out1, "last") {
		t.Errorf("last-done row was removed from the block: %q", out1)
	}
	if strings.Contains(out1, "\x1b[") {
		t.Errorf("first draw must not move the cursor or clear: %q", out1)
	}

	out2 := captureStdout(t, func() { r.printStatus(time.Now()) })
	if !strings.Contains(out2, "\x1b[2A") {
		t.Errorf("second repaint must step up blockLines-1 rows, got %q", out2)
	}
	if got := strings.Count(out2, "\x1b[2K"); got != 3 {
		t.Errorf("repaint must clear every block row (%d), got %d: %q", 3, got, out2)
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

func TestFmtDurShort(t *testing.T) {
	cases := []struct {
		d    time.Duration
		want string
	}{
		{0, "0s"},
		{7 * time.Second, "7s"},
		{59 * time.Second, "59s"},
		{60 * time.Second, "1:00"},
		{1200*time.Second + 30*time.Second, "20:30"},
		{3599 * time.Second, "59:59"},
		{3600 * time.Second, "1:00:00"},
		{5422 * time.Second, "1:30:22"}, // the nina 5.2 GB AVI case: was "5422s"
	}
	for _, c := range cases {
		if got := fmtDurShort(c.d); got != c.want {
			t.Errorf("fmtDurShort(%v) = %q, want %q", c.d, got, c.want)
		}
	}
}

func TestStatusBlockSlots(t *testing.T) {
	r := &Runner{multi: true, processRoot: `C:\arc`, cfg: &config.Config{}}
	r.cfg.Workers = 2
	r.trackStart(`C:\arc\family\holiday.mp4`)
	r.trackStart(`C:\arc\family\second very long file name with a lot of characters here.mp4`)
	lines := r.statusBlock("hdr", 200)
	if len(lines) != 3 {
		t.Fatalf("want header + 2 slots, got %d: %q", len(lines), lines)
	}
	if !strings.Contains(lines[1], `family\holiday.mp4`) {
		t.Errorf("slot 1 must show the relative path: %q", lines[1])
	}
	// time comes FIRST (fixed column), then the path
	if idxT, idxP := strings.Index(lines[1], "0s"), strings.Index(lines[1], `family\`); idxT == -1 || idxP == -1 || idxT > idxP {
		t.Errorf("elapsed time must precede the path: %q", lines[1])
	}

	r.trackEnd(`C:\arc\family\holiday.mp4`)
	lines = r.statusBlock("hdr", 200)
	if strings.Contains(lines[1]+lines[2], "holiday.mp4") {
		t.Errorf("finished file still in a worker slot: %q", lines)
	}
	if !strings.Contains(lines[2], "--") {
		t.Errorf("freed slot must fall back to dashes: %q", lines)
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

func TestStatusSlotProgress(t *testing.T) {
	r := &Runner{multi: true, processRoot: `C:\arc`, cfg: &config.Config{Workers: 1}}
	r.trackStart(`C:\arc\family\nina.mkv`)
	rel := `family\nina.mkv`

	// no encode yet: middle column empty, path still visible
	lines := r.statusBlock("hdr", 200)
	if !strings.Contains(lines[1], `family\nina.mkv`) {
		t.Fatalf("slot lost the path: %q", lines[1])
	}

	// lane wait must be visible (the battle complaint: "stuck or moving?")
	r.trackProgress(rel, "queued", 0)
	lines = r.statusBlock("hdr", 200)
	if !strings.Contains(lines[1], "queued") {
		t.Errorf("lane wait must show queued: %q", lines[1])
	}

	// encode pass with known duration: position/total in the slot
	r.trackTotal(rel, 802_000) // 13:22 of media (the nina case, scaled down)
	r.trackProgress(rel, "crf28", 271_000)
	lines = r.statusBlock("hdr", 200)
	if !strings.Contains(lines[1], "crf28 4:31/13:22") {
		t.Errorf("encode position column wrong: %q", lines[1])
	}

	// unknown duration: position alone still proves movement
	r2 := &Runner{multi: true, processRoot: `C:\arc`, cfg: &config.Config{Workers: 1}}
	r2.trackStart(`C:\arc\family\nina.mkv`)
	r2.trackProgress(rel, "crf34", 65_000)
	lines = r2.statusBlock("hdr", 200)
	if !strings.Contains(lines[1], "crf34 1:05") || strings.Contains(lines[1], "/") {
		t.Errorf("unknown-duration pass must show position only: %q", lines[1])
	}

	// over-1h media (the nina 1:30:22 case): full h:mm:ss pair must fit the
	// column without pushing the path out of alignment
	r3 := &Runner{multi: true, processRoot: `C:\arc`, cfg: &config.Config{Workers: 2}}
	r3.trackStart(`C:\arc\family\nina.avi`)
	r3.trackTotal(`family\nina.avi`, 5_422_000)
	r3.trackProgress(`family\nina.avi`, "crf28", 4_591_000)
	r3.trackStart(`C:\arc\x.jpg`)
	lines = r3.statusBlock("hdr", 200)
	want := "crf28 1:16:31/1:30:22" // 21 chars: exactly the column width
	if !strings.Contains(lines[1], want+`  family\nina.avi`) {
		t.Errorf("over-1h position must fit the column: %q", lines[1])
	}
}

func TestAchievedForEncoders(t *testing.T) {
	r := &Runner{opts: media.Opts{VideoPreset: 8, PhotoQ: 85}}
	if got := r.achievedFor(detect.KindVideo, media.VideoResult{CRF: 28, VMAF: -1, Codec: "svt"}, media.PhotoResult{}); got != "svt p8 crf=28" {
		t.Errorf("svt achieved must carry codec+preset+crf: %q", got)
	}
	if got := r.achievedFor(detect.KindVideo, media.VideoResult{CRF: 34, VMAF: 96.4, Codec: "svt"}, media.PhotoResult{}); got != "svt p8 crf=34 vmaf=96.4" {
		t.Errorf("svt achieved must carry vmaf too: %q", got)
	}
	// nvenc ignores --video-preset: no p-marker may be recorded for it
	if got := r.achievedFor(detect.KindVideo, media.VideoResult{CRF: 28, VMAF: -1, Codec: "nvenc"}, media.PhotoResult{}); got != "nvenc crf=28" {
		t.Errorf("gpu winner must not carry a preset marker: %q", got)
	}
	// preset 0 = codec default: nothing to record
	r0 := &Runner{opts: media.Opts{}}
	if got := r0.achievedFor(detect.KindVideo, media.VideoResult{CRF: 28, VMAF: -1, Codec: "svt"}, media.PhotoResult{}); got != "svt crf=28" {
		t.Errorf("preset 0 must not add a marker: %q", got)
	}
	if got := r.achievedFor(detect.KindImage, media.VideoResult{}, media.PhotoResult{Q: 85, VMAF: -1, Codec: "avifenc"}); got != "avifenc q=85" {
		t.Errorf("photo achieved: %q", got)
	}
}

func TestVideoLanesClamp(t *testing.T) {
	// explicit lanes never exceed workers; 0 means auto (env-dependent, so
	// only assert it is at least 1 and within workers)
	if got := videoLanes(&config.Config{VideoWorkers: 5, Workers: 3}); got != 3 {
		t.Errorf("lanes must clamp to workers: %d", got)
	}
	if got := videoLanes(&config.Config{VideoWorkers: -4, Workers: 2}); got != 2 {
		t.Errorf("auto lanes must clamp to workers: %d", got)
	}
	if got := videoLanes(&config.Config{VideoWorkers: 0, Workers: 0}); got != 1 {
		t.Errorf("lanes floor must be 1: %d", got)
	}
	if got := DefaultVideoLanes(); got < 1 {
		t.Errorf("DefaultVideoLanes must be >= 1: %d", got)
	}
}

func TestVideosLast(t *testing.T) {
	in := []string{
		`a\clip.MP4`, `a\photo.jpg`, `a\movie.mkv`, `a\notes.txt`, `a\clip.webm`, `a\pic.jpeg`,
	}
	got := videosLast(in)
	want := []string{`a\photo.jpg`, `a\notes.txt`, `a\pic.jpeg`, `a\clip.MP4`, `a\movie.mkv`, `a\clip.webm`}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("videosLast = %v, want %v", got, want)
			break
		}
	}
}
