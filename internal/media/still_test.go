package media

import (
	"context"
	"encoding/binary"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/Llloooggg/ImageCompressor/internal/tools"
)

// --- pure tests: pixel metric ----------------------------------------------

func TestMad8(t *testing.T) {
	a := []byte{10, 20, 30, 40}
	b := []byte{12, 18, 34, 40} // diffs: 2+2+4+0 = 8 / 4 = 2
	if got := mad8(a, b); got != 2 {
		t.Errorf("mad8 = %d, want 2", got)
	}
	if got := mad8(a, []byte{10, 20, 30}); got != 256 {
		t.Errorf("mad8 mismatched lengths = %d, want 256", got)
	}
	if got := mad8(a, a); got != 0 {
		t.Errorf("mad8(same) = %d, want 0", got)
	}
}

// --- pure tests: handcrafted MP4 box tables --------------------------------

func box4(name string, payload []byte) []byte {
	b := make([]byte, 8+len(payload))
	binary.BigEndian.PutUint32(b[0:4], uint32(len(b)))
	copy(b[4:8], name)
	copy(b[8:], payload)
	return b
}

func cat(bs ...[]byte) []byte {
	var out []byte
	for _, b := range bs {
		out = append(out, b...)
	}
	return out
}

func mvhdSeconds(sec float64) []byte {
	p := make([]byte, 100)
	binary.BigEndian.PutUint32(p[0:4], 0) // version 0, no flags
	binary.BigEndian.PutUint32(p[12:16], 1000)
	binary.BigEndian.PutUint32(p[16:20], uint32(sec*1000))
	return box4("mvhd", p)
}

func stszSizes(sizes []uint32) []byte {
	p := make([]byte, 12+4*len(sizes))
	binary.BigEndian.PutUint32(p[4:8], 0) // uniform size 0 => per-sample table follows
	binary.BigEndian.PutUint32(p[8:12], uint32(len(sizes)))
	for i, s := range sizes {
		binary.BigEndian.PutUint32(p[12+4*i:], s)
	}
	return box4("stsz", p)
}

func stszUniform(size uint32, count int) []byte {
	p := make([]byte, 12)
	binary.BigEndian.PutUint32(p[4:8], size)
	binary.BigEndian.PutUint32(p[8:12], uint32(count))
	return box4("stsz", p)
}

func stssList(entries []uint32) []byte {
	p := make([]byte, 8+4*len(entries))
	binary.BigEndian.PutUint32(p[4:8], uint32(len(entries)))
	for i, s := range entries {
		binary.BigEndian.PutUint32(p[8+4*i:], s)
	}
	return box4("stss", p)
}

func trakVideo(stblPayload ...[]byte) []byte {
	hdlr := make([]byte, 24) // ver/flags + predefined + handler_type ("vide")
	copy(hdlr[8:12], "vide")
	stbl := make([]byte, 0)
	for _, b := range stblPayload {
		stbl = append(stbl, b...)
	}
	return box4("trak", cat(box4("hdlr", hdlr), box4("stbl", stbl)))
}

func writeFakeMP4(t *testing.T, moovPayload []byte) string {
	t.Helper()
	// ftyp + mdat placeholder + moov: moov sits BEHIND mdat, so the parser
	// must exercise its seek-over-huge-box path.
	file := append(box4("ftyp", []byte("isom\x00\x00\x02\x00isom")),
		box4("mdat", make([]byte, 4096))...)
	file = append(file, box4("moov", moovPayload)...)
	p := filepath.Join(t.TempDir(), "fake.mp4")
	if err := os.WriteFile(p, file, 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestProbeMP4VideoPerSample(t *testing.T) {
	p := writeFakeMP4(t, append(
		mvhdSeconds(4),
		trakVideo(
			stszSizes([]uint32{80000, 400, 250, 12288, 260}), // keyframes 1 and 4
			stssList([]uint32{1, 4}),
		)...))
	v, err := probeMP4Video(p)
	if err != nil {
		t.Fatal(err)
	}
	if v.samples != 5 {
		t.Errorf("samples = %d, want 5", v.samples)
	}
	if v.maxInter != 400 {
		t.Errorf("maxInter = %d, want 400 (keyframes 1 and 4 excluded)", v.maxInter)
	}
	if v.duration != 4 {
		t.Errorf("duration = %v, want 4", v.duration)
	}
}

func TestProbeMP4VideoNoStssAllIntra(t *testing.T) {
	p := writeFakeMP4(t, append(mvhdSeconds(2),
		trakVideo(stszSizes([]uint32{100000, 90, 80}))...))
	v, err := probeMP4Video(p)
	if err != nil {
		t.Fatal(err)
	}
	if v.maxInter != 90 { // everything but sample 1 counts as inter
		t.Errorf("maxInter = %d, want 90", v.maxInter)
	}
}

func TestProbeMP4VideoUniform(t *testing.T) {
	p := writeFakeMP4(t, append(mvhdSeconds(3),
		trakVideo(stszUniform(350, 30), stssList([]uint32{1, 15, 29}))...))
	v, err := probeMP4Video(p)
	if err != nil {
		t.Fatal(err)
	}
	if v.samples != 30 || v.maxInter != 350 {
		t.Errorf("got samples=%d maxInter=%d, want 30/350", v.samples, v.maxInter)
	}
}

func TestProbeMP4VideoFragmentedRejected(t *testing.T) {
	p := writeFakeMP4(t, append(append(mvhdSeconds(3),
		trakVideo(stszUniform(100, 60))...), box4("mvex", make([]byte, 16))...))
	if _, err := probeMP4Video(p); err == nil {
		t.Error("fragmented mp4 must be rejected, got nil error")
	}
}

func TestWatchdogRejectsBeforePixelPass(t *testing.T) {
	// One fat inter sample: StillVideo must answer false even without any
	// ffmpeg involvement (bitstream witness alone is enough to refuse).
	p := writeFakeMP4(t, append(mvhdSeconds(3),
		trakVideo(stszSizes([]uint32{50000, 300, 300, 20000, 300}), stssList([]uint32{1, 3}))...))
	loc := tools.NewLocator()
	if StillVideo(context.Background(), loc, p) {
		t.Error("fat inter packet must veto collapse")
	}
}

func TestWatchdogAcceptsTinyStatic(t *testing.T) {
	// bitstream side only: fake file passes stsz but has no real frames, so
	// probeMP4Video numbers must be hat-shaped.
	p := writeFakeMP4(t, append(mvhdSeconds(3),
		trakVideo(stszSizes([]uint32{50000, 300, 300, 260, 280, 270, 300, 250, 200, 300,
			300, 300, 260, 280, 270, 300, 250, 200, 300, 300, 260, 280, 270, 300, 250}),
			stssList([]uint32{1, 13}))...))
	v, err := probeMP4Video(p)
	if err != nil {
		t.Fatal(err)
	}
	if v.samples < stillMinSamples || v.maxInter >= stillMaxPacket {
		t.Errorf("expected hat-shaped numbers, got samples=%d maxInter=%d", v.samples, v.maxInter)
	}
}

// --- integration tests: real ffmpeg on synthetic clips ---------------------

func testFFmpeg(t *testing.T) (*tools.Locator, string) {
	t.Helper()
	loc := tools.NewLocator()
	ff := loc.FFmpeg()
	if ff == "" {
		if _, err := exec.LookPath("ffmpeg"); err != nil {
			t.Skip("no ffmpeg (embedded or PATH), integration test skipped")
		}
	}
	return loc, ff
}

func makeClip(t *testing.T, ff, dir, name, src, vf string, withAudio bool) string {
	t.Helper()
	out := filepath.Join(dir, name)
	if src == "" {
		src = "color=c=0x7f807f:s=320x240:d=4:r=30"
	}
	args := []string{"-y", "-hide_banner", "-loglevel", "error",
		"-f", "lavfi", "-i", src}
	if withAudio {
		args = append(args, "-f", "lavfi", "-i", "sine=frequency=220:duration=4")
	}
	if vf != "" {
		args = append(args, "-vf", vf)
	}
	args = append(args, "-c:v", "libx264", "-preset", "ultrafast", "-crf", "30",
		"-g", "120", "-pix_fmt", "yuv420p")
	if withAudio {
		args = append(args, "-c:a", "aac", "-shortest")
	}
	args = append(args, out)
	if b, err := exec.Command(ff, args...).CombinedOutput(); err != nil {
		t.Fatalf("generate %s: %v\n%s", name, err, b)
	}
	return out
}

func TestStillVideoSyntheticClasses(t *testing.T) {
	loc, ff := testFFmpeg(t)
	dir := t.TempDir()
	ctx := context.Background()

	cases := []struct {
		name string
		src  string
		vf   string
		want bool
	}{
		// pure hat: one gray frame + soundtrack
		{"hat.mp4", "", "", true},
		// hat with a tiny blinking sticker: absorbed on purpose (8x8 over
		// 320x240 is 0.7% area: dilutes under MAD 6)
		{"hat_sticker.mp4", "", `drawbox=x=8:y=8:w=8:h=8:color=white@1:t=fill:enable='lt(mod(t\,1)\,0.5)'`, true},
		// real motion: testsrc scrolls content
		{"motion.mp4", "testsrc=size=320x240:rate=30:duration=4", "", false},
		// slideshow: whole frame swaps at t=2 (consecutive-diff witness)
		{"slideshow.mp4", "", `drawbox=x=0:y=0:w=iw:h=ih:color=red@1:t=fill:enable='gte(t\,2)'`, false},
	}
	for _, c := range cases {
		p := makeClip(t, ff, dir, c.name, c.src, c.vf, true)
		if got := StillVideo(ctx, loc, p); got != c.want {
			v, perr := probeMP4Video(p)
			pixel, ferr := framesNearlyStill(ctx, ff, p)
			t.Errorf("%s: StillVideo = %v, want %v (probe: samples=%d maxInter=%d err=%v; pixel=%v err=%v)",
				c.name, got, c.want, v.samples, v.maxInter, perr, pixel, ferr)
		}
	}
}

func TestStillVideoSlowZoomRejected(t *testing.T) {
	loc, ff := testFFmpeg(t)
	dir := t.TempDir()
	// Ken Burns style, built the way social apps do it: ONE still frame is
	// expanded by zoompan into 120 frames (z accumulates within one input
	// frame group; d=1 per frame would reset it). Consecutive frames barely
	// differ (sub-pixel drift), the first-vs-current comparison must catch
	// the accumulated zoom.
	out := filepath.Join(dir, "zoom.mp4")
	args := []string{"-y", "-hide_banner", "-loglevel", "error",
		"-f", "lavfi", "-i", "color=c=0x7f807f:s=320x240:d=1:r=1",
		"-vf", "drawgrid=w=16:h=12:t=4:c=black,zoompan=z='min(zoom+0.0017,1.2)':d=120:s=320x240:fps=30",
		"-c:v", "libx264", "-preset", "ultrafast", "-crf", "30", "-g", "120", out}
	if b, err := exec.Command(ff, args...).CombinedOutput(); err != nil {
		t.Fatalf("generate zoom: %v\n%s", err, b)
	}
	if StillVideo(context.Background(), loc, out) {
		t.Error("slow zoom must not be classified as a hat")
	}
}

func TestCollapseStillEndToEnd(t *testing.T) {
	loc, ff := testFFmpeg(t)
	dir := t.TempDir()
	hat := makeClip(t, ff, dir, "hat.mp4", "", "", true)
	if !StillVideo(context.Background(), loc, hat) {
		t.Fatal("test clip must register as a hat before collapse is tested")
	}
	origSize, err := os.Stat(hat)
	if err != nil {
		t.Fatal(err)
	}
	tmp := filepath.Join(dir, "out.avif")
	res, err := CollapseStill(context.Background(), loc, Opts{PhotoQ: 85, PhotoSpeed: 6}, hat, tmp, origSize.Size(), nil)
	if err != nil {
		t.Fatal(err)
	}
	st, err := os.Stat(tmp)
	if err != nil {
		t.Fatal(err)
	}
	if st.Size() == 0 || st.Size() >= origSize.Size() {
		t.Errorf("collapsed avif size %d vs original %d: must be smaller and non-empty", st.Size(), origSize.Size())
	}
	if res.Q != 85 {
		t.Errorf("achieved Q = %d, want base 85", res.Q)
	}
	// the PNG intermediate must not linger
	pe, _ := filepath.Glob(filepath.Join(dir, ".mc-tmp-*.png"))
	if len(pe) > 0 {
		t.Errorf("PNG intermediates left behind: %v", pe)
	}
}
