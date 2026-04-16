package archiveprogress

import (
	"bytes"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/islishude/gotgz/packages/cli"
)

func TestProgressReporterKnownTotalIncludesETAAndElapsed(t *testing.T) {
	var buf bytes.Buffer
	p := NewReporter(&buf, cli.ProgressAlways, 100, true, time.Now().Add(-2*time.Second), false)
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
	if !strings.Contains(out, "elapsed") {
		t.Fatalf("expected elapsed output, got %q", out)
	}
	if !strings.HasSuffix(out, "\n") {
		t.Fatalf("expected trailing newline, got %q", out)
	}
}

func TestProgressReporterUnknownTotalOmitsETA(t *testing.T) {
	var buf bytes.Buffer
	p := NewReporter(&buf, cli.ProgressAlways, 0, false, time.Now().Add(-2*time.Second), false)
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
	p := NewReporter(&buf, cli.ProgressAuto, 10, true, time.Now().Add(-time.Second), false)
	p.AddDone(10)
	p.Finish()

	if got := buf.String(); got != "" {
		t.Fatalf("expected no output for non-tty auto mode, got %q", got)
	}
}

func TestProgressReporterBeforeExternalLineOutput(t *testing.T) {
	var buf bytes.Buffer
	p := NewReporter(&buf, cli.ProgressAlways, 100, true, time.Now().Add(-time.Second), false)
	p.AddDone(10)
	p.BeforeExternalLineOutput()
	out := buf.String()
	if !strings.HasSuffix(out, "\n") {
		t.Fatalf("expected newline after breaking progress line, got %q", out)
	}
}

func TestProgressReporterFinishIdempotent(t *testing.T) {
	var buf bytes.Buffer
	p := NewReporter(&buf, cli.ProgressAlways, 100, true, time.Now().Add(-time.Second), false)
	p.AddDone(100)
	p.Finish()
	first := buf.String()
	p.Finish()
	if got := buf.String(); got != first {
		t.Fatalf("second Finish() changed output: %q vs %q", first, got)
	}
}

func TestProgressReporterNilSafe(t *testing.T) {
	var p *Reporter
	// These should not panic on a nil receiver.
	p.SetTotal(100, true)
	p.AddDone(50)
	p.BeforeExternalLineOutput()
	p.AfterExternalLineOutput()
	var buf bytes.Buffer
	p.ExternalLinef(&buf, "hello %s\n", "world")
	p.Finish()
	if got := buf.String(); got != "hello world\n" {
		t.Fatalf("ExternalLinef() = %q, want %q", got, "hello world\n")
	}
	if got := p.Elapsed(); got != 0 {
		t.Fatalf("Elapsed() = %v, want 0 for nil reporter", got)
	}
}

func TestProgressReporterExternalLinef(t *testing.T) {
	var buf bytes.Buffer
	p := NewReporter(&buf, cli.ProgressAlways, 100, true, time.Now().Add(-time.Second), false)
	p.AddDone(10)
	p.ExternalLinef(&buf, "external %s\n", "line")

	out := buf.String()
	if !strings.Contains(out, "external line\n") {
		t.Fatalf("expected formatted external line, got %q", out)
	}
}

func TestReporterEnabled(t *testing.T) {
	t.Run("nil reporter", func(t *testing.T) {
		var p *Reporter
		if got := p.Enabled(); got {
			t.Fatalf("Enabled() = %v, want false for nil reporter", got)
		}
	})

	t.Run("disabled reporter", func(t *testing.T) {
		p := NewReporter(io.Discard, cli.ProgressNever, 100, true, time.Now(), false)
		if got := p.Enabled(); got {
			t.Fatalf("Enabled() = %v, want false", got)
		}
	})

	t.Run("enabled reporter", func(t *testing.T) {
		p := NewReporter(io.Discard, cli.ProgressAlways, 100, true, time.Now(), false)
		if got := p.Enabled(); !got {
			t.Fatalf("Enabled() = %v, want true", got)
		}
	})
}

func TestReporterElapsed(t *testing.T) {
	t.Run("running reporter uses start time", func(t *testing.T) {
		p := NewReporter(io.Discard, cli.ProgressAlways, 100, true, time.Now().Add(-150*time.Millisecond), false)
		if got := p.Elapsed(); got < 100*time.Millisecond {
			t.Fatalf("Elapsed() = %v, want at least 100ms", got)
		}
	})

	t.Run("finished disabled reporter freezes elapsed", func(t *testing.T) {
		p := NewReporter(io.Discard, cli.ProgressNever, 100, true, time.Now().Add(-150*time.Millisecond), false)
		p.Finish()
		first := p.Elapsed()
		if first < 100*time.Millisecond {
			t.Fatalf("Elapsed() after Finish() = %v, want at least 100ms", first)
		}

		time.Sleep(20 * time.Millisecond)
		if got := p.Elapsed(); got != first {
			t.Fatalf("Elapsed() changed after Finish(): first=%v second=%v", first, got)
		}
	})
}

func TestProgressReporterSetTotal(t *testing.T) {
	t.Run("enabled reporter updates totals and renders", func(t *testing.T) {
		var buf bytes.Buffer
		p := NewReporter(&buf, cli.ProgressAlways, 100, true, time.Now().Add(-time.Second), false)
		p.SetTotal(2048, true)

		if p.total != 2048 {
			t.Fatalf("total = %d, want 2048", p.total)
		}
		if !p.totalKnown {
			t.Fatalf("totalKnown = %v, want true", p.totalKnown)
		}
		if got := buf.String(); !strings.Contains(got, "/2.0KiB") {
			t.Fatalf("expected rendered output to include new total, got %q", got)
		}
	})

	t.Run("negative total is clamped to zero", func(t *testing.T) {
		var buf bytes.Buffer
		p := NewReporter(&buf, cli.ProgressAlways, 100, true, time.Now().Add(-time.Second), false)
		p.SetTotal(-1, true)

		if p.total != 0 {
			t.Fatalf("total = %d, want 0", p.total)
		}
		if !p.totalKnown {
			t.Fatalf("totalKnown = %v, want true", p.totalKnown)
		}
	})

	t.Run("disabled reporter is no-op", func(t *testing.T) {
		var buf bytes.Buffer
		p := NewReporter(&buf, cli.ProgressNever, 100, true, time.Now(), false)
		p.SetTotal(2048, true)

		if p.total != 100 {
			t.Fatalf("total = %d, want unchanged 100", p.total)
		}
		if !p.totalKnown {
			t.Fatalf("totalKnown = %v, want unchanged true", p.totalKnown)
		}
		if got := buf.String(); got != "" {
			t.Fatalf("expected no output for disabled reporter, got %q", got)
		}
	})
}

func TestNewCountingReaderBypassesDisabledReporter(t *testing.T) {
	src := strings.NewReader("payload")
	reporter := NewReporter(io.Discard, cli.ProgressAuto, 0, false, time.Now(), false)
	if got := NewCountingReader(src, reporter); got != src {
		t.Fatalf("NewCountingReader() should return original reader when progress is disabled")
	}
}

func TestNewCountingReadCloserBypassesDisabledReporter(t *testing.T) {
	src := io.NopCloser(strings.NewReader("payload"))
	reporter := NewReporter(io.Discard, cli.ProgressAuto, 0, false, time.Now(), false)
	if got := NewCountingReadCloser(src, reporter); got != src {
		t.Fatalf("NewCountingReadCloser() should return original reader when progress is disabled")
	}
}

// newTopPinnedReporter creates a ProgressReporter with topPinned=true for
// testing the ANSI cursor-management code path that requires an interactive
// TTY. Since bytes.Buffer is not a real TTY, we construct the struct directly.
func newTopPinnedReporter(buf *bytes.Buffer, total int64, totalKnown bool, start time.Time) *Reporter {
	return &Reporter{
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
	p.lastDrawUnix.Store(0)
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
	p.AfterExternalLineOutput()
	p.AfterExternalLineOutput()
	after := buf.String()
	if before != after {
		t.Fatalf("AfterExternalLineOutput should be no-op for topPinned, before=%q after=%q", before, after)
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
	p.BeforeExternalLineOutput()
	if got := buf.String(); !strings.Contains(got, "\033[2;999r\033[999;1H") {
		t.Fatalf("expected scroll region setup, got %q", got)
	}
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
