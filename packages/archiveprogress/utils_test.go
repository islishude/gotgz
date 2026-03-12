package archiveprogress

import (
	"io"
	"os"
	"testing"
	"time"
)

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
			if got := FormatBytes(tc.input); got != tc.want {
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
