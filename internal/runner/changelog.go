package runner

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

// changeLog is an append-only audit file (TSV, Excel-friendly) that lives next
// to the DB: what was converted into what, size before/after, achieved quality.
// One row per mutation (сжат / не уменьшился / исправлено расширение / ошибка).
// Rows are flushed immediately so the log survives even a process kill.
type changeLog struct {
	mu sync.Mutex
	f  *os.File
	w  *bufio.Writer
}

const changeLogHeader = "Дата\tДействие\tИсходник\tРезультат\tБыло,байт\tСтало,байт\tЭкономия,байт\tТип\tФормат\tКачество\tКомментарий"

// openChangeLog appends to path (UTF-8 BOM + header written once for Excel).
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
		_ = cl.w.Flush()
	}
	return cl, nil
}

// record writes one audit row. Tab/newline chars in fields are flattened so
// the TSV column structure can never be corrupted by file names or messages.
func (c *changeLog) record(action, src, dst string, srcSize, dstSize int64, kind, format, achieved, msg string) {
	if c == nil {
		return
	}
	saved := srcSize - dstSize
	if saved < 0 {
		saved = 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	fmt.Fprintf(c.w, "%s\t%s\t%s\t%s\t%d\t%d\t%d\t%s\t%s\t%s\t%s\n",
		time.Now().Format("2006-01-02 15:04:05"), flat(action), flat(src), flat(dst),
		srcSize, dstSize, saved, flat(kind), flat(format), flat(achieved), flat(msg))
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
