package runner

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

// changeLog is an append-only audit log next to the DB: what was converted
// into what, size before/after, achieved quality. Plain text, columns split by
// tabs (opens fine in Notepad; Excel can too, but nothing depends on it).
// One row per mutation (compressed / not smaller / ext renamed / error).
// Rows are flushed immediately so the log survives even a process kill.
type changeLog struct {
	mu sync.Mutex
	f  *os.File
	w  *bufio.Writer
}

const changeLogHeader = "Date\tAction\tSource\tResult\tSrcMB\tDstMB\tSavedMB\tSeconds\tKind\tFormat\tQuality\tComment"

// openChangeLog appends to path. A brand-new file gets the UTF-8 BOM + header;
// an existing file gets a blank separator line + a fresh header on every run,
// so each run's rows sit under their own labeled section and the boundary
// between runs is obvious when scrolling (the header always reflects the
// current column layout too, so a format change is self-labeling).
func openChangeLog(path string) (*changeLog, error) {
	fresh := false
	if _, err := os.Stat(path); err != nil {
		fresh = true
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	cl := &changeLog{f: f, w: bufio.NewWriter(f)}
	if fresh {
		cl.w.WriteString("\ufeff" + changeLogHeader + "\n")
	} else {
		cl.w.WriteString("\n" + changeLogHeader + "\n")
	}
	_ = cl.w.Flush()
	return cl, nil
}

// record writes one audit row. Tab/newline chars in fields are flattened so
// the TSV column structure can never be corrupted by file names or messages.
// Sizes go out in MB (user request: raw byte counts were unreadable across a
// 40k-line log); secs is how long this file took to process.
func (c *changeLog) record(action, src, dst string, srcSize, dstSize int64, secs float64, kind, format, achieved, msg string) {
	if c == nil {
		return
	}
	saved := srcSize - dstSize
	if saved < 0 {
		saved = 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	fmt.Fprintf(c.w, "%s\t%s\t%s\t%s\t%.3f\t%.3f\t%.3f\t%.1f\t%s\t%s\t%s\t%s\n",
		time.Now().Format("2006-01-02 15:04:05"), flat(action), flat(src), flat(dst),
		mbOf(srcSize), mbOf(dstSize), mbOf(saved), secs, flat(kind), flat(format), flat(achieved), flat(msg))
	_ = c.w.Flush()
}

func mbOf(n int64) float64 { return float64(n) / (1024 * 1024) }

// summary writes the end-of-run console summary as one log line (newlines
// flattened), so the run's verdict survives even if the console window gets
// closed before it can be read.
func (c *changeLog) summary(text string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	fmt.Fprintf(c.w, "%s\tsummary\t%s\n", time.Now().Format("2006-01-02 15:04:05"), flat(text))
	_ = c.w.Flush()
}

func (c *changeLog) close() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	_ = c.w.Flush()
	_ = c.f.Close()
}

func flat(s string) string {
	return strings.NewReplacer("\t", " ", "\n", " ", "\r", " ").Replace(s)
}
