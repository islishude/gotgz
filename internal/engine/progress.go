package engine

import (
	"fmt"
	"io"
	"math"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/islishude/gotgz/internal/cli"
)

const (
	progressRefreshInterval = 200 * time.Millisecond
	progressBarWidth        = 20
)

// progressReporter tracks bytes processed and renders progress updates.
type progressReporter struct {
	mu          sync.Mutex
	writer      io.Writer
	enabled     bool
	topPinned   bool
	total       int64
	totalKnown  bool
	done        int64
	startTime   time.Time
	lastDraw    time.Time
	cursorBelow int
	rendered    bool
	finished    bool
}

// newProgressReporter creates a progress reporter configured for the requested mode.
func newProgressReporter(writer io.Writer, mode cli.ProgressMode, totalBytes int64, totalKnown bool, startTime time.Time) *progressReporter {
	if startTime.IsZero() {
		startTime = time.Now()
	}
	if totalBytes < 0 {
		totalBytes = 0
	}
	interactive := isInteractiveTTY(writer)
	enabled := shouldEnableProgress(mode, writer, interactive)
	return &progressReporter{
		writer:     writer,
		enabled:    enabled,
		topPinned:  enabled && interactive,
		total:      totalBytes,
		totalKnown: totalKnown,
		startTime:  startTime,
	}
}

// SetTotal updates the total byte estimate used by progress and ETA.
func (p *progressReporter) SetTotal(total int64, known bool) {
	if p == nil {
		return
	}
	if total < 0 {
		total = 0
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	p.total = total
	p.totalKnown = known
	p.renderLocked(false)
}

// AddDone increments processed bytes and triggers a throttled refresh.
func (p *progressReporter) AddDone(n int64) {
	if p == nil || n <= 0 {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	p.done += n
	p.renderLocked(false)
}

// Finish forces a final render and terminates the progress line.
func (p *progressReporter) Finish() {
	if p == nil {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.enabled || p.finished {
		return
	}
	p.renderLocked(true)
	if p.rendered && !p.topPinned {
		_, _ = fmt.Fprint(p.writer, "\n")
	}
	p.finished = true
}

// beforeExternalLineOutput prepares terminal state before printing external output.
func (p *progressReporter) beforeExternalLineOutput() {
	if p == nil {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.enabled || p.finished || !p.rendered || p.topPinned {
		return
	}
	_, _ = fmt.Fprint(p.writer, "\n")
	p.rendered = false
	p.lastDraw = time.Time{}
}

// afterExternalLineOutput records that one non-progress line was printed.
func (p *progressReporter) afterExternalLineOutput() {
	if p == nil {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.enabled || p.finished || !p.topPinned || !p.rendered {
		return
	}
	p.cursorBelow++
}

// renderLocked prints one progress line if refresh throttling allows it.
func (p *progressReporter) renderLocked(force bool) {
	if !p.enabled {
		return
	}
	now := time.Now()
	if !force && !p.lastDraw.IsZero() && now.Sub(p.lastDraw) < progressRefreshInterval {
		return
	}

	line := p.formatLine(now)
	if p.topPinned {
		if !p.rendered {
			// First render claims the top line and leaves cursor below it.
			_, _ = fmt.Fprintf(p.writer, "\r%s\033[K\n", line)
			p.cursorBelow = 1
		} else {
			up := max(p.cursorBelow, 1)
			// Save cursor, jump back to progress line, update, then restore.
			_, _ = fmt.Fprintf(p.writer, "\033[s\033[%dA\r%s\033[K\033[u", up, line)
		}
		p.lastDraw = now
		p.rendered = true
		return
	}

	// Use ANSI escape to clear the rest of the line so that shorter lines
	// do not leave residual characters from the previous render.
	_, _ = fmt.Fprintf(p.writer, "\r%s\033[K", line)
	p.lastDraw = now
	p.rendered = true
}

// formatLine renders one line with either known-total or unknown-total layout.
func (p *progressReporter) formatLine(now time.Time) string {
	elapsed := max(now.Sub(p.startTime), time.Millisecond)
	speed := float64(p.done) / elapsed.Seconds()

	if p.totalKnown {
		if p.total <= 0 {
			return "gotgz: [####################] 100.0% 0B/0B 0B/s ETA 00:00"
		}

		ratio := float64(p.done) / float64(p.total)
		if ratio < 0 {
			ratio = 0
		}
		if ratio > 1 {
			ratio = 1
		}
		filled := min(max(int(math.Round(ratio*progressBarWidth)), 0), progressBarWidth)

		bar := strings.Repeat("#", filled) + strings.Repeat(".", progressBarWidth-filled)
		var eta time.Duration
		if speed > 0 && p.done < p.total {
			remaining := float64(p.total-p.done) / speed
			eta = time.Duration(remaining * float64(time.Second))
		}
		return fmt.Sprintf(
			"gotgz: [%s] %5.1f%% %s/%s %s/s ETA %s",
			bar,
			ratio*100,
			formatBytes(p.done),
			formatBytes(p.total),
			formatRate(speed),
			formatClock(eta),
		)
	}

	return fmt.Sprintf(
		"gotgz: [working] %s processed %s/s elapsed %s",
		formatBytes(p.done),
		formatRate(speed),
		formatClock(elapsed),
	)
}

// shouldEnableProgress decides whether progress output is active for this run.
func shouldEnableProgress(mode cli.ProgressMode, writer io.Writer, interactive bool) bool {
	if writer == nil {
		return false
	}
	switch mode {
	case cli.ProgressNever:
		return false
	case cli.ProgressAlways:
		return true
	case "", cli.ProgressAuto:
		return interactive
	default:
		return interactive
	}
}

// isInteractiveTTY checks whether the writer is an interactive terminal.
// Only the "dumb" TERM value disables detection; an empty TERM (common in
// containers) is accepted as long as the fd is a character device.
func isInteractiveTTY(writer io.Writer) bool {
	file, ok := writer.(*os.File)
	if !ok {
		return false
	}
	info, err := file.Stat()
	if err != nil {
		return false
	}
	if info.Mode()&os.ModeCharDevice == 0 {
		return false
	}
	if strings.EqualFold(strings.TrimSpace(os.Getenv("TERM")), "dumb") {
		return false
	}
	return true
}

// formatBytes formats bytes using IEC units.
func formatBytes(v int64) string {
	if v < 0 {
		v = 0
	}
	units := []string{"B", "KiB", "MiB", "GiB", "TiB", "PiB"}
	value := float64(v)
	unit := 0
	for value >= 1024 && unit < len(units)-1 {
		value /= 1024
		unit++
	}
	if unit == 0 {
		return fmt.Sprintf("%dB", v)
	}
	return fmt.Sprintf("%.1f%s", value, units[unit])
}

// formatRate formats byte-per-second throughput values.
func formatRate(v float64) string {
	if v <= 0 {
		return "0B"
	}
	units := []string{"B", "KiB", "MiB", "GiB", "TiB", "PiB"}
	value := v
	unit := 0
	for value >= 1024 && unit < len(units)-1 {
		value /= 1024
		unit++
	}
	if unit == 0 {
		return fmt.Sprintf("%.0fB", value)
	}
	return fmt.Sprintf("%.1f%s", value, units[unit])
}

// formatClock formats a duration as MM:SS or HH:MM:SS.
func formatClock(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	d = d.Round(time.Second)
	h := int(d / time.Hour)
	m := int((d % time.Hour) / time.Minute)
	s := int((d % time.Minute) / time.Second)
	if h > 0 {
		return fmt.Sprintf("%02d:%02d:%02d", h, m, s)
	}
	return fmt.Sprintf("%02d:%02d", m, s)
}

// countingReader reports bytes read to a progress reporter.
type countingReader struct {
	reader   io.Reader
	reporter *progressReporter
}

// newCountingReader wraps a reader and records all successful reads.
func newCountingReader(reader io.Reader, reporter *progressReporter) io.Reader {
	return &countingReader{reader: reader, reporter: reporter}
}

// Read reads from the underlying reader and records progress.
func (r *countingReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if n > 0 {
		r.reporter.AddDone(int64(n))
	}
	return n, err
}

// countingReadCloser wraps an io.ReadCloser and reports bytes read to a
// progress reporter. It embeds countingReader to avoid duplicating the Read
// implementation.
type countingReadCloser struct {
	countingReader
	closer io.Closer
}

// newCountingReadCloser wraps a read closer and records all successful reads.
func newCountingReadCloser(reader io.ReadCloser, reporter *progressReporter) io.ReadCloser {
	return &countingReadCloser{
		countingReader: countingReader{reader: reader, reporter: reporter},
		closer:         reader,
	}
}

// Close closes the wrapped reader.
func (r *countingReadCloser) Close() error {
	return r.closer.Close()
}
