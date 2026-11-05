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

// openChangeLog appends to path (UTF-8 BOM + header written once so any
// viewer shows Cyrillic correctly and the column names are visible).
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
	} else if !headerMatches(path) {
		// Column layout changed between program versions (bytes became MB,
		// a Seconds column was added): restate the header exactly where the
		// new-style rows begin so nobody misreads MB rows as byte rows.
		cl.w.WriteString(changeLogHeader + "\n")
	}
	_ = cl.w.Flush()
	return cl, nil
}

// headerMatches reports whether the existing log's first line already uses the
// current column layout.
func headerMatches(path string) bool {
	f, err := os.Open(path)
	if err != nil {
		return true // unreadable: don't risk duplicating the header
	}
	defer f.Close()
	line, err := bufio.NewReader(f).ReadString('\n')
	if err != nil && line == "" {
		return true
	}
	return strings.TrimRight(strings.TrimPrefix(line, "\ufeff"), "\r\n") == changeLogHeader
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
