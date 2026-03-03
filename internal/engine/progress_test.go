package engine

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/islishude/gotgz/internal/cli"
)

func TestProgressReporterKnownTotalIncludesETA(t *testing.T) {
	var buf bytes.Buffer
	p := newProgressReporter(&buf, cli.ProgressAlways, 100, true, time.Now().Add(-2*time.Second))
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
	p := newProgressReporter(&buf, cli.ProgressAlways, 0, false, time.Now().Add(-2*time.Second))
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
	p := newProgressReporter(&buf, cli.ProgressAuto, 10, true, time.Now().Add(-time.Second))
	p.AddDone(10)
	p.Finish()

	if got := buf.String(); got != "" {
		t.Fatalf("expected no output for non-tty auto mode, got %q", got)
	}
}

func TestProgressReporterBeforeExternalLineOutput(t *testing.T) {
	var buf bytes.Buffer
	p := newProgressReporter(&buf, cli.ProgressAlways, 100, true, time.Now().Add(-time.Second))
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
	p := newProgressReporter(&buf, cli.ProgressAlways, 100, true, time.Now().Add(-time.Second))
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
