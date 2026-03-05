package engine

import (
	"bytes"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/islishude/gotgz/internal/cli"
)

func TestProgressReporterKnownTotalIncludesETA(t *testing.T) {
	var buf bytes.Buffer
	p := newProgressReporter(&buf, cli.ProgressAlways, 100, true, time.Now().Add(-2*time.Second), false)
	p.AddDone(50)
	p.Finish()

	out := buf.String()
	if !strings.Contains(out, "gotgz:") {
		t.Fatalf("expected progress prefix, got %q", out)
	}
	if !strings.Contains(out, "%") {
		t.Fatalf("expected percentage output, got %q", out)
	}
	if !strings.Contains(out, "ETA") {
		t.Fatalf("expected ETA output, got %q", out)
	}
	if !strings.HasSuffix(out, "\n") {
		t.Fatalf("expected trailing newline, got %q", out)
	}
}

func TestProgressReporterUnknownTotalOmitsETA(t *testing.T) {
	var buf bytes.Buffer
	p := newProgressReporter(&buf, cli.ProgressAlways, 0, false, time.Now().Add(-2*time.Second), false)
	p.AddDone(512)
	p.Finish()

	out := buf.String()
	if !strings.Contains(out, "processed") {
		t.Fatalf("expected processed output, got %q", out)
	}
	if !strings.Contains(out, "elapsed") {
		t.Fatalf("expected elapsed output, got %q", out)
	}
	if strings.Contains(out, "ETA") {
		t.Fatalf("did not expect ETA output, got %q", out)
	}
}

func TestProgressReporterAutoDisablesOnNonTTY(t *testing.T) {
	var buf bytes.Buffer
	p := newProgressReporter(&buf, cli.ProgressAuto, 10, true, time.Now().Add(-time.Second), false)
	p.AddDone(10)
	p.Finish()

	if got := buf.String(); got != "" {
		t.Fatalf("expected no output for non-tty auto mode, got %q", got)
	}
}

func TestProgressReporterBeforeExternalLineOutput(t *testing.T) {
	var buf bytes.Buffer
	p := newProgressReporter(&buf, cli.ProgressAlways, 100, true, time.Now().Add(-time.Second), false)
	p.AddDone(10)
	p.beforeExternalLineOutput()
	out := buf.String()
	if !strings.HasSuffix(out, "\n") {
		t.Fatalf("expected newline after breaking progress line, got %q", out)
	}
}

func TestFormatBytes(t *testing.T) {
	cases := []struct {
		input int64
		want  string
	}{
		{0, "0B"},
		{-1, "0B"},
		{1, "1B"},
		{1023, "1023B"},
		{1024, "1.0KiB"},
		{1536, "1.5KiB"},
		{1048576, "1.0MiB"},
		{1073741824, "1.0GiB"},
		{1099511627776, "1.0TiB"},
		{1125899906842624, "1.0PiB"},
	}
	for _, tc := range cases {
		t.Run(tc.want, func(t *testing.T) {
			if got := formatBytes(tc.input); got != tc.want {
				t.Fatalf("formatBytes(%d) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

func TestFormatRate(t *testing.T) {
	cases := []struct {
		input float64
		want  string
	}{
		{0, "0B"},
		{-10, "0B"},
		{500, "500B"},
		{1024, "1.0KiB"},
		{1048576, "1.0MiB"},
		{1536.0, "1.5KiB"},
	}
	for _, tc := range cases {
		t.Run(tc.want, func(t *testing.T) {
			if got := formatRate(tc.input); got != tc.want {
				t.Fatalf("formatRate(%f) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

func TestFormatClock(t *testing.T) {
	cases := []struct {
		input time.Duration
		want  string
	}{
		{0, "00:00"},
		{-5 * time.Second, "00:00"},
		{30 * time.Second, "00:30"},
		{90 * time.Second, "01:30"},
		{3600 * time.Second, "01:00:00"},
		{3661 * time.Second, "01:01:01"},
		{500 * time.Millisecond, "00:01"},
	}
	for _, tc := range cases {
		t.Run(tc.want, func(t *testing.T) {
			if got := formatClock(tc.input); got != tc.want {
				t.Fatalf("formatClock(%v) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

func TestProgressReporterFinishIdempotent(t *testing.T) {
	var buf bytes.Buffer
	p := newProgressReporter(&buf, cli.ProgressAlways, 100, true, time.Now().Add(-time.Second), false)
	p.AddDone(100)
	p.Finish()
	first := buf.String()
	p.Finish()
	if got := buf.String(); got != first {
		t.Fatalf("second Finish() changed output: %q vs %q", first, got)
	}
}

func TestProgressReporterNilSafe(t *testing.T) {
	var p *progressReporter
	// These should not panic on a nil receiver.
	p.SetTotal(100, true)
	p.AddDone(50)
	p.beforeExternalLineOutput()
	p.afterExternalLineOutput()
	p.Finish()
}

// newTopPinnedReporter creates a progressReporter with topPinned=true for
// testing the ANSI cursor-management code path that requires an interactive
// TTY. Since bytes.Buffer is not a real TTY, we construct the struct directly.
func newTopPinnedReporter(buf *bytes.Buffer, total int64, totalKnown bool, start time.Time) *progressReporter {
	return &progressReporter{
		writer:       buf,
		enabled:      true,
		topPinned:    true,
		total:        total,
		totalKnown:   totalKnown,
		startTime:    start,
		scrollRegion: false,
	}
}

func TestTopPinnedFirstRenderSetsScrollRegionAndUsesSaveRestore(t *testing.T) {
	var buf bytes.Buffer
	p := newTopPinnedReporter(&buf, 100, true, time.Now().Add(-time.Second))
	p.AddDone(50)

	out := buf.String()
	if !strings.Contains(out, "\033[2;999r\033[999;1H") {
		t.Fatalf("expected scroll region setup, got %q", out)
	}
	if !strings.Contains(out, "\0337\033[1;1H") {
		t.Fatalf("expected cursor save + move to row 1, got %q", out)
	}
	if !strings.Contains(out, "\033[K\0338") {
		t.Fatalf("expected clear-to-eol + cursor restore, got %q", out)
	}
	if !strings.Contains(out, "gotgz:") {
		t.Fatalf("expected progress prefix, got %q", out)
	}
}

func TestTopPinnedSubsequentRenderDoesNotRecreateScrollRegion(t *testing.T) {
	var buf bytes.Buffer
	p := newTopPinnedReporter(&buf, 100, true, time.Now().Add(-time.Second))

	// First render.
	p.AddDone(10)
	buf.Reset()

	// Force a second render by clearing the throttle and adding bytes.
	p.mu.Lock()
	p.lastDraw = time.Time{}
	p.mu.Unlock()
	p.AddDone(10)

	out := buf.String()
	if strings.Contains(out, "\033[2;999r\033[999;1H") {
		t.Fatalf("did not expect a second scroll-region setup, got %q", out)
	}
	if !strings.Contains(out, "\0337\033[1;1H") {
		t.Fatalf("expected cursor save + move to row 1, got %q", out)
	}
	if !strings.Contains(out, "\033[K\0338") {
		t.Fatalf("expected clear-to-EOL + cursor restore, got %q", out)
	}
}

func TestAfterExternalLineOutputNoOpForTopPinned(t *testing.T) {
	var buf bytes.Buffer
	p := newTopPinnedReporter(&buf, 100, true, time.Now().Add(-time.Second))
	p.AddDone(10)
	before := buf.String()
	p.afterExternalLineOutput()
	p.afterExternalLineOutput()
	after := buf.String()
	if before != after {
		t.Fatalf("afterExternalLineOutput should be no-op for topPinned, before=%q after=%q", before, after)
	}
}

func TestTopPinnedFinishResetsScrollRegion(t *testing.T) {
	var buf bytes.Buffer
	p := newTopPinnedReporter(&buf, 100, true, time.Now().Add(-time.Second))
	p.AddDone(100)
	p.Finish()

	out := buf.String()
	if !strings.Contains(out, "\033[r\033[999;1H") {
		t.Fatalf("expected scroll region reset, got %q", out)
	}
	if strings.HasSuffix(out, "\n\n") {
		t.Fatalf("topPinned Finish should not double-newline, got %q", out)
	}
}

func TestBeforeExternalLineOutputInitializesTopPinnedRegion(t *testing.T) {
	var buf bytes.Buffer
	p := newTopPinnedReporter(&buf, 100, true, time.Now().Add(-time.Second))
	p.beforeExternalLineOutput()
	if got := buf.String(); !strings.Contains(got, "\033[2;999r\033[999;1H") {
		t.Fatalf("expected scroll region setup, got %q", got)
	}
}

func Test_isInteractiveTTY(t *testing.T) {
	t.Run("non-file writer", func(t *testing.T) {
		if got := isInteractiveTTY(io.Discard); got {
			t.Fatalf("isInteractiveTTY(io.Discard) = %v, want false", got)
		}
	})

	t.Run("pipe writer is non-interactive", func(t *testing.T) {
		r, w, err := os.Pipe()
		if err != nil {
			t.Fatalf("os.Pipe() error = %v", err)
		}
		defer func() {
			_ = r.Close()
			_ = w.Close()
		}()

		if got := isInteractiveTTY(w); got {
			t.Fatalf("isInteractiveTTY(pipe writer) = %v, want false", got)
		}
	})

	t.Run("dumb term disables tty", func(t *testing.T) {
		ttyFile := findTTYFile(t)
		if ttyFile == nil {
			t.Skip("no character device available in stdio for tty detection tests")
		}
		t.Setenv("TERM", "dumb")
		if got := isInteractiveTTY(ttyFile); got {
			t.Fatalf("isInteractiveTTY(ttyFile) = %v, want false when TERM=dumb", got)
		}
	})

	t.Run("empty TERM still counts as interactive", func(t *testing.T) {
		ttyFile := findTTYFile(t)
		if ttyFile == nil {
			t.Skip("no character device available in stdio for tty detection tests")
		}
		t.Setenv("TERM", "")
		if got := isInteractiveTTY(ttyFile); !got {
			t.Fatalf("isInteractiveTTY(ttyFile) = %v, want true when TERM is empty", got)
		}
	})

	t.Run("char device with non-dumb TERM is interactive", func(t *testing.T) {
		ttyFile := findTTYFile(t)
		if ttyFile == nil {
			t.Skip("no character device available in stdio for tty detection tests")
		}
		t.Setenv("TERM", "xterm-256color")
		if got := isInteractiveTTY(ttyFile); !got {
			t.Fatalf("isInteractiveTTY(ttyFile) = %v, want true", got)
		}
	})
}

func findTTYFile(t *testing.T) *os.File {
	t.Helper()
	for _, candidate := range []*os.File{os.Stdin, os.Stdout, os.Stderr} {
		if candidate == nil {
			continue
		}
		info, err := candidate.Stat()
		if err != nil {
			t.Logf("stat %q: %v", candidate.Name(), err)
			continue
		}
		if info.Mode()&os.ModeCharDevice != 0 {
			return candidate
		}
		t.Logf("%q mode=%s is not a character device", candidate.Name(), info.Mode())
	}
	return nil
}

func TestShouldEnableProgress(t *testing.T) {
	tests := []struct {
		name        string
		mode        cli.ProgressMode
		writer      io.Writer
		interactive bool
		want        bool
	}{
		{
			name:        "nil writer always disabled",
			mode:        cli.ProgressAlways,
			writer:      nil,
			interactive: true,
			want:        false,
		},
		{
			name:        "never disables",
			mode:        cli.ProgressNever,
			writer:      io.Discard,
			interactive: true,
			want:        false,
		},
		{
			name:        "always enables on non-nil writer",
			mode:        cli.ProgressAlways,
			writer:      io.Discard,
			interactive: false,
			want:        true,
		},
		{
			name:        "auto enables only when interactive",
			mode:        cli.ProgressAuto,
			writer:      io.Discard,
			interactive: true,
			want:        true,
		},
		{
			name:        "auto disables when non-interactive",
			mode:        cli.ProgressAuto,
			writer:      io.Discard,
			interactive: false,
			want:        false,
		},
		{
			name:        "empty mode behaves like auto",
			mode:        "",
			writer:      io.Discard,
			interactive: true,
			want:        true,
		},
		{
			name:        "unknown mode behaves like auto",
			mode:        cli.ProgressMode("custom"),
			writer:      io.Discard,
			interactive: false,
			want:        false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := shouldEnableProgress(tt.mode, tt.writer, tt.interactive); got != tt.want {
				t.Fatalf("shouldEnableProgress(%q, writer, %v) = %v, want %v", tt.mode, tt.interactive, got, tt.want)
			}
		})
	}
}
