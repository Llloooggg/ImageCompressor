// Still-image video ("hat") support: social apps glue one static frame to a
// soundtrack and ship the result as MP4. Such files are photos wearing a
// video costume: the bitstream carries no motion at all and the audio is
// decoration. We detect them (two independent witnesses) and collapse them
// into a single photo through the regular photo pipeline, dropping the
// soundtrack by design.
//
// Detection witnesses, both must agree (a miss costs nothing — the file just
// goes through the normal video ladder; a false positive must be impossible):
//
//  1. Bitstream watchdog, pure Go, millisecond cost, covers the WHOLE
//     timeline without decoding: parse the MP4/MOV sample table (stsz/stss)
//     and look at the LARGEST inter-frame packet. A static picture encodes to
//     packets of tens of bytes (nothing to transmit); a sticker or caption
//     over a static background reaches maybe a kilobyte; real motion anywhere
//     in the file produces tens of kilobytes. Orders of magnitude apart.
//
//  2. Pixel verification of candidates only (the cheap filter already
//     eliminated ~all real videos, so paying a full decode here is fine):
//     scale every decoded frame to 128x128 gray and require every frame to
//     match both its predecessor AND the first frame within a small MAD.
//     Consecutive diffs catch transitions (slideshow), first-vs-current diffs
//     catch slow drift (Ken Burns zoom) that consecutive diffs alone would
//     hide. A tiny animated sticker dilutes under the threshold and is
//     absorbed on purpose — the user decided sticker garnish is not content
//     worth keeping.
package media

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"

	"github.com/Llloooggg/ImageCompressor/internal/safefs"
	"github.com/Llloooggg/ImageCompressor/internal/tools"
)

const (
	// stillMaxPacket: an inter-frame sample at/above this size means real
	// motion exists somewhere in the timeline (static floor is ~10^2-3*10^2 B,
	// sticker/caption residuals ~10^3 B, real movement ~10^4 B).
	stillMaxPacket = 12 * 1024
	// stillMaxMAD: mean per-pixel absolute difference (0..255) at 128x128 gray.
	// Encoder noise on a still is <=3, small stickers/plates 1-5, zooms and
	// real video >=15.
	stillMaxMAD = 6
	// stillMinSamples: too few frames for either witness to mean anything —
	// leave the file to the normal video path.
	stillMinSamples = 20
	// stillMaxFrames caps the pixel pass for absurd durations (sanity guard).
	stillMaxFrames = 300000
)

// StillVideo reports whether src (an MP4/MOV container) is really one static
// picture with audio glued on. Anything unparseable, fragmented, tiny or
// doubtful returns false — the caller then uses the normal video ladder.
func StillVideo(ctx context.Context, loc *tools.Locator, src string) bool {
	v, err := probeMP4Video(src)
	if err != nil {
		return false
	}
	if v.samples < stillMinSamples || v.maxInter >= stillMaxPacket {
		return false
	}
	ff := loc.FFmpeg()
	if ff == "" {
		return false // no pixel witness available: never collapse on bitstream alone
	}
	ok, err := framesNearlyStill(ctx, ff, src)
	return err == nil && ok
}

// CollapseStill turns a StillVideo-confirmed file into one AVIF photo:
// mid-duration frame -> PNG intermediate -> the regular photo encoder
// (ladder/guard semantics included; the base level almost always wins since
// it is compared against the VIDEO size). The soundtrack is deliberately
// discarded. tmp must end in .avif; the original is never touched.
func CollapseStill(ctx context.Context, loc *tools.Locator, o Opts, src, tmp string, origSize int64) (PhotoResult, error) {
	ff := loc.FFmpeg()
	if ff == "" {
		return PhotoResult{}, errors.New("no ffmpeg to extract a frame from video")
	}
	v, err := probeMP4Video(src)
	if err != nil {
		return PhotoResult{}, fmt.Errorf("still frame position: %w", err)
	}
	mid := v.duration / 2
	if mid < 0 || v.duration <= 0 {
		mid = 0
	}
	// Intermediate frame goes into a .mc-tmp-*.png beside the source: if we
	// die here the next run's CleanTemps sweeps it (IsTempName matches).
	png, err := safefs.TempName(src + ".png")
	if err != nil {
		return PhotoResult{}, fmt.Errorf("temp frame: %w", err)
	}
	defer os.Remove(png)
	// -ss before -i: near-keyframe seek, then ffmpeg decodes-and-discards to
	// the exact timestamp (accurate seek). Middle frame avoids fade-in first
	// frames social apps like to add.
	if err := run(loc, ff, "-hide_banner", "-loglevel", "error",
		"-ss", strconv.FormatFloat(mid, 'f', 3, 64), "-i", src,
		"-frames:v", "1", png); err != nil {
		return PhotoResult{}, fmt.Errorf("extract still frame: %w", err)
	}
	if st, serr := os.Stat(png); serr != nil || st.Size() == 0 {
		return PhotoResult{}, errors.New("extract still frame: no frame produced")
	}
	res, err := EncodeImage(ctx, loc, o, png, tmp, origSize)
	return res, err
}

// --- witness 2: pixel pass -------------------------------------------------

// framesNearlyStill decodes the whole video (candidates only — cheap!) and
// checks every frame against both its predecessor and the first frame.
func framesNearlyStill(ctx context.Context, ff, src string) (bool, error) {
	cmd := exec.CommandContext(ctx, ff, "-hide_banner", "-loglevel", "error",
		"-i", src, "-vf", "scale=128:128:flags=area,format=gray", "-f", "rawvideo", "-")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	pipe, err := cmd.StdoutPipe()
	if err != nil {
		return false, err
	}
	if err := cmd.Start(); err != nil {
		return false, err
	}
	const frameSize = 128 * 128
	var first, prev, cur []byte
	frames, worst := 0, 0
	done := false
	for !done {
		if ctx.Err() != nil {
			cmd.Process.Kill()
			cmd.Wait()
			return false, ctx.Err()
		}
		cur = make([]byte, frameSize)
		if _, err := io.ReadFull(pipe, cur); err != nil {
			break // EOF or a short trailing frame
		}
		frames++
		if frames == 1 {
			first = cur
		} else {
			if m := mad8(cur, prev); m > worst {
				worst = m
			}
			if m := mad8(cur, first); m > worst {
				worst = m
			}
			if worst > stillMaxMAD {
				cmd.Process.Kill() // verdict reached; stop the decode
				done = true
			}
		}
		prev = cur
		if frames >= stillMaxFrames {
			cmd.Process.Kill()
			done = true
		}
	}
	werr := cmd.Wait()
	if worst > stillMaxMAD {
		return false, nil // decisive: there is motion
	}
	if frames < stillMinSamples {
		return false, nil
	}
	if werr != nil {
		return false, fmt.Errorf("pixel pass: %w (%s)", werr, stderr.String())
	}
	return true, nil
}

// mad8 is the mean absolute pixel difference of two equal-length gray frames,
// on the 0..255 scale.
func mad8(a, b []byte) int {
	if len(a) != len(b) {
		return 256
	}
	sum := 0
	for i := range a {
		d := int(a[i]) - int(b[i])
		if d < 0 {
			d = -d
		}
		sum += d
	}
	return sum / len(a)
}

// --- witness 1: bitstream watchdog (pure Go, no decoding) ------------------

// mp4Video is what the still detector needs from an MP4/MOV sample table.
type mp4Video struct {
	samples  int   // video-track sample count
	maxInter int64 // largest inter (non-keyframe) sample size in bytes
	duration float64
}

// probeMP4Video walks the box tree (headers only, seeking over mdat) and
// extracts the first video track's sample-size statistics. Fragmented or
// malformed inputs return an error, which callers treat as "not a hat".
func probeMP4Video(path string) (mp4Video, error) {
	f, err := os.Open(path)
	if err != nil {
		return mp4Video{}, err
	}
	defer f.Close()
	size, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return mp4Video{}, err
	}

	var v mp4Video
	err = walkBoxes(f, 0, size, func(name string, pStart, pEnd int64) (bool, error) {
		if name != "moov" {
			return false, nil
		}
		if pEnd <= pStart || pEnd-pStart > 1<<28 {
			return false, errors.New("moov header too large")
		}
		if err := parseMoov(f, pStart, pEnd, &v); err != nil {
			return false, err
		}
		return true, nil // stop walking
	})
	if err != nil {
		return mp4Video{}, err
	}
	if v.samples == 0 {
		return mp4Video{}, errors.New("no plain mp4/mov video track")
	}
	return v, nil
}

// walkBoxes iterates the box headers in [start,end), invoking fn for each.
// fn returns stop=true to end the walk early. Unknown box payloads are
// skipped by seeking, so huge mdat regions cost nothing.
func walkBoxes(f io.ReadSeeker, start, end int64, fn func(name string, payloadStart, payloadEnd int64) (bool, error)) error {
	pos := start
	for pos+8 <= end {
		if _, err := f.Seek(pos, io.SeekStart); err != nil {
			return err
		}
		var hdr [8]byte
		if _, err := io.ReadFull(f, hdr[:]); err != nil {
			return nil // truncated tail: stop
		}
		sz := int64(binary.BigEndian.Uint32(hdr[:4]))
		name := string(hdr[4:8])
		pStart := pos + 8
		switch {
		case sz == 1: // 64-bit largesize follows
			var big [8]byte
			if _, err := io.ReadFull(f, big[:]); err != nil {
				return nil
			}
			sz = int64(binary.BigEndian.Uint64(big[:]))
			pStart += 8
		case sz == 0: // box extends to end of parent
			sz = end - pos
		}
		if sz < 8 {
			return fmt.Errorf("bad box %q size %d", name, sz)
		}
		pEnd := pos + sz
		if pEnd > end {
			pEnd = end
		}
		stop, err := fn(name, pStart, pEnd)
		if err != nil {
			return err
		}
		if stop {
			return nil
		}
		pos = pEnd
	}
	return nil
}

func parseMoov(f io.ReadSeeker, moovStart, moovEnd int64, v *mp4Video) error {
	var fragmented bool
	var videoErr error
	err := walkBoxes(f, moovStart, moovEnd, func(name string, pStart, pEnd int64) (bool, error) {
		switch name {
		case "mvhd":
			d, err := parseMvhd(f, pStart, pEnd)
			if err == nil {
				v.duration = d
			}
		case "mvex":
			fragmented = true
		case "trak":
			if videoErr != nil { // a video track was already parsed
				return false, nil
			}
			if err := parseTrak(f, pStart, pEnd, v); err != nil {
				videoErr = err
			}
		}
		return false, nil
	})
	if fragmented {
		return errors.New("fragmented mp4 not supported")
	}
	if videoErr != nil {
		return videoErr
	}
	return err
}

// parseTrak inspects one track: only the first VIDEO track feeds the detector.
// NOTE: per spec stbl lives at trak/mdia/minf/stbl, not directly under trak —
// findBox descends through the container chain.
func parseTrak(f io.ReadSeeker, start, end int64, v *mp4Video) error {
	hdlr, _, err := findBox(f, start, end, "hdlr", 0)
	if err != nil {
		return err
	}
	if hdlr == 0 {
		return nil
	}
	h, err := parseHdlr(f, hdlr, 0)
	if err != nil {
		return err
	}
	if h != "vide" {
		return nil
	}
	stblStart, stblEnd, err := findBox(f, start, end, "stbl", 0)
	if err != nil {
		return err
	}
	if stblStart == 0 {
		return nil
	}
	return parseStbl(f, stblStart, stblEnd, v)
}

// containerBoxes are the MP4 structural wrappers we may descend into.
var containerBoxes = map[string]bool{
	"moov": true, "trak": true, "mdia": true, "minf": true, "stbl": true,
	"edts": true, "dinf": true, "mvex": true,
}

// findBox locates the first box named name inside [start,end), descending
// through known container wrappers. Returns (0,0,nil) when absent; boxes
// over 64 MB of payload are never descended into.
func findBox(f io.ReadSeeker, start, end int64, name string, depth int) (int64, int64, error) {
	if depth > 8 {
		return 0, 0, nil
	}
	var got [2]int64
	found := false
	err := walkBoxes(f, start, end, func(n string, s, e int64) (bool, error) {
		if n == name {
			got = [2]int64{s, e}
			found = true
			return true, nil
		}
		if containerBoxes[n] && e-s <= 1<<26 {
			ps, pe, err := findBox(f, s, e, name, depth+1)
			if err != nil {
				return false, err
			}
			if pe != 0 {
				got = [2]int64{ps, pe}
				found = true
				return true, nil
			}
		}
		return false, nil
	})
	if !found {
		return 0, 0, err
	}
	return got[0], got[1], err
}

// parseStbl pulls sample sizes (stsz) and the sync-sample list (stss).
func parseStbl(f io.ReadSeeker, start, end int64, v *mp4Video) error {
	var sizes []uint32
	var uniform uint32
	var sampleCount int
	var syncs []uint32
	var hasStss bool

	err := walkBoxes(f, start, end, func(name string, pStart, pEnd int64) (bool, error) {
		if _, err := f.Seek(pStart, io.SeekStart); err != nil {
			return false, err
		}
		switch name {
		case "stsz":
			var head [12]byte
			if _, err := io.ReadFull(f, head[:]); err != nil {
				return false, err
			}
			uniform = binary.BigEndian.Uint32(head[4:8])
			n := int(binary.BigEndian.Uint32(head[8:12]))
			if n <= 0 || n > 20_000_000 {
				return false, fmt.Errorf("implausible sample count %d", n)
			}
			sampleCount = n
			if uniform == 0 {
				sizes = make([]uint32, n)
				buf := make([]byte, 4*n)
				if _, err := io.ReadFull(f, buf); err != nil {
					return false, err
				}
				for i := 0; i < n; i++ {
					sizes[i] = binary.BigEndian.Uint32(buf[4*i:])
				}
			}
		case "stss":
			var cnt [4]byte
			var verflags [4]byte
			if _, err := io.ReadFull(f, verflags[:]); err != nil {
				return false, err
			}
			if _, err := io.ReadFull(f, cnt[:]); err != nil {
				return false, err
			}
			n := int(binary.BigEndian.Uint32(cnt[:]))
			if n < 0 || n > 20_000_000 {
				return false, fmt.Errorf("implausible sync count %d", n)
			}
			hasStss = true
			syncs = make([]uint32, n)
			buf := make([]byte, 4*n)
			if _, err := io.ReadFull(f, buf); err != nil {
				return false, err
			}
			for i := 0; i < n; i++ {
				syncs[i] = binary.BigEndian.Uint32(buf[4*i:])
			}
		}
		return false, nil
	})
	if err != nil {
		return err
	}

	// Inter-sample stats: everything that is not a keyframe (stss lists
	// 1-based sync samples). Without stss every sample is sync — then every
	// sample except the first is treated as inter, which is the
	// conservative reading (all-intra videos then fail the watchdog, as
	// they should: their I-frames are big by nature).
	v.samples = sampleCount
	v.maxInter = 0
	if sizes != nil { // per-sample table
		si := 0
		for i, s := range sizes {
			n1 := uint32(i + 1)
			if hasStss {
				for si < len(syncs) && syncs[si] < n1 {
					si++
				}
				if si < len(syncs) && syncs[si] == n1 {
					continue // keyframe
				}
			} else if i == 0 {
				continue
			}
			if int64(s) > v.maxInter {
				v.maxInter = int64(s)
			}
		}
		return nil
	}
	if uniform != 0 { // single size for every sample
		inter := sampleCount
		if hasStss {
			inter = sampleCount - len(syncs)
		} else {
			inter = sampleCount - 1
		}
		if inter > 0 {
			v.maxInter = int64(uniform)
		}
	}
	return nil
}

func parseHdlr(f io.ReadSeeker, start, end int64) (string, error) {
	if _, err := f.Seek(start, io.SeekStart); err != nil {
		return "", err
	}
	var b [12]byte
	if _, err := io.ReadFull(f, b[:]); err != nil {
		return "", err
	}
	return string(b[8:12]), nil
}

func parseMvhd(f io.ReadSeeker, start, end int64) (float64, error) {
	if _, err := f.Seek(start, io.SeekStart); err != nil {
		return 0, err
	}
	var b [32]byte
	if _, err := io.ReadFull(f, b[:]); err != nil {
		return 0, err
	}
	if b[0] == 1 { // v1: 64-bit times, 64-bit duration
		ts := binary.BigEndian.Uint32(b[20:24])
		dur := binary.BigEndian.Uint64(b[24:32])
		if ts == 0 {
			return 0, errors.New("mvhd v1 timescale 0")
		}
		return float64(dur) / float64(ts), nil
	}
	ts := binary.BigEndian.Uint32(b[12:16])
	dur := binary.BigEndian.Uint32(b[16:20])
	if ts == 0 {
		return 0, errors.New("mvhd timescale 0")
	}
	return float64(dur) / float64(ts), nil
}
