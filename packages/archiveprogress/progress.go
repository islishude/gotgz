package archiveprogress

import (
	"fmt"
	"io"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/islishude/gotgz/packages/cli"
)

const (
	RefreshInterval = 200 * time.Millisecond
	BarWidth        = 20
)

// Reporter tracks bytes processed and renders progress updates.
type Reporter struct {
	mu           sync.Mutex
	writer       io.Writer
	enabled      bool
	topPinned    bool
	scrollRegion bool
	total        int64
	totalKnown   bool
	done         atomic.Int64
	startTime    time.Time
	lastDraw     time.Time
	lastDrawUnix atomic.Int64
	rendered     bool
	finished     bool
}

// NewReporter creates a progress reporter configured for the requested mode.
// pinTop reserves the first terminal row for progress updates when interactive output
// also prints additional lines (for example, verbose file names).
func NewReporter(writer io.Writer, mode cli.ProgressMode, totalBytes int64, totalKnown bool, startTime time.Time, pinTop bool) *Reporter {
	if startTime.IsZero() {
		startTime = time.Now()
	}
	if totalBytes < 0 {
		totalBytes = 0
	}
	interactive := isInteractiveTTY(writer)
	enabled := shouldEnableProgress(mode, writer, interactive)
	return &Reporter{
		writer:     writer,
		enabled:    enabled,
		topPinned:  enabled && interactive && pinTop,
		total:      totalBytes,
		totalKnown: totalKnown,
		startTime:  startTime,
	}
}

// SetTotal updates the total byte estimate used by progress rendering.
func (p *Reporter) SetTotal(total int64, known bool) {
	if p == nil || !p.enabled {
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
func (p *Reporter) AddDone(n int64) {
	if p == nil || n <= 0 || !p.enabled {
		return
	}
	p.done.Add(n)

	lastDraw := p.lastDrawUnix.Load()
	if lastDraw != 0 && time.Now().UnixNano()-lastDraw < int64(RefreshInterval) {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.finished {
		return
	}
	p.renderLocked(false)
}

// Finish forces a final render and terminates the progress line.
func (p *Reporter) Finish() {
	if p == nil {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.enabled || p.finished {
		return
	}
	p.renderLocked(true)
	if p.topPinned {
		p.resetScrollRegionLocked()
	} else if p.rendered {
		_, _ = fmt.Fprint(p.writer, "\n")
	}
	p.finished = true
}

// BeforeExternalLineOutput prepares terminal state before printing external output.
func (p *Reporter) BeforeExternalLineOutput() {
	if p == nil {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.enabled || p.finished {
		return
	}
	if p.topPinned {
		p.ensureScrollRegionLocked()
		return
	}
	if !p.rendered {
		return
	}
	_, _ = fmt.Fprintln(p.writer)
	p.rendered = false
	p.lastDraw = time.Time{}
	p.lastDrawUnix.Store(0)
}

// AfterExternalLineOutput records that one non-progress line was printed.
func (p *Reporter) AfterExternalLineOutput() {
	if p == nil {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.enabled || p.finished || !p.topPinned || !p.rendered {
		return
	}
}

// renderLocked prints one progress line if refresh throttling allows it.
func (p *Reporter) renderLocked(force bool) {
	if !p.enabled {
		return
	}
	now := time.Now()
	lastDrawUnix := p.lastDrawUnix.Load()
	if !force && lastDrawUnix != 0 && now.UnixNano()-lastDrawUnix < int64(RefreshInterval) {
		return
	}

	line := p.formatLine(now)
	if p.topPinned {
		p.ensureScrollRegionLocked()
		// Save cursor, draw status on line 1, and restore the original cursor.
		_, _ = fmt.Fprintf(p.writer, "\0337\033[1;1H%s\033[K\0338", line)
		p.lastDraw = now
		p.lastDrawUnix.Store(now.UnixNano())
		p.rendered = true
		return
	}

	// Use ANSI escape to clear the rest of the line so that shorter lines
	// do not leave residual characters from the previous render.
	_, _ = fmt.Fprintf(p.writer, "\r%s\033[K", line)
	p.lastDraw = now
	p.lastDrawUnix.Store(now.UnixNano())
	p.rendered = true
}

// ensureScrollRegionLocked reserves the first terminal row for progress.
func (p *Reporter) ensureScrollRegionLocked() {
	if p.scrollRegion {
		return
	}
	// Keep row 1 fixed and let normal output scroll in rows [2..bottom].
	// 999 is intentionally oversized; terminals clamp it to the actual size.
	_, _ = fmt.Fprint(p.writer, "\033[2;999r\033[999;1H")
	p.scrollRegion = true
}

// resetScrollRegionLocked restores default terminal scrolling.
func (p *Reporter) resetScrollRegionLocked() {
	if !p.scrollRegion {
		return
	}
	_, _ = fmt.Fprint(p.writer, "\033[r\033[999;1H")
	p.scrollRegion = false
}

// formatLine renders one line with either known-total or unknown-total layout.
func (p *Reporter) formatLine(now time.Time) string {
	elapsed := max(now.Sub(p.startTime), time.Millisecond)
	done := p.done.Load()
	speed := float64(done) / elapsed.Seconds()

	if p.totalKnown {
		if p.total <= 0 {
			return "gotgz: [####################] 100.0% 0B/0B 0B/s ETA 00:00 elapsed 00:00"
		}

		ratio := float64(done) / float64(p.total)
		if ratio < 0 {
			ratio = 0
		}
		if ratio > 1 {
			ratio = 1
		}
		filled := min(max(int(math.Round(ratio*BarWidth)), 0), BarWidth)

		bar := strings.Repeat("#", filled) + strings.Repeat(".", BarWidth-filled)
		var eta time.Duration
		if speed > 0 && done < p.total {
			remaining := float64(p.total-done) / speed
			eta = time.Duration(remaining * float64(time.Second))
		}
		return fmt.Sprintf(
			"gotgz: [%s] %5.1f%% %s/%s %s/s ETA %s elapsed %s",
			bar,
			ratio*100,
			FormatBytes(done),
			FormatBytes(p.total),
			formatRate(speed),
			formatClock(eta),
			formatClock(elapsed),
		)
	}

	return fmt.Sprintf(
		"gotgz: [working] %s processed %s/s elapsed %s",
		FormatBytes(done),
		formatRate(speed),
		formatClock(elapsed),
	)
}

// Enabled reports whether progress updates are active.
func (p *Reporter) Enabled() bool {
	return p != nil && p.enabled
}
