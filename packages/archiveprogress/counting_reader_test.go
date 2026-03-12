package archiveprogress

import (
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/islishude/gotgz/packages/cli"
)

func TestNewCountingReaderWrapsEnabledReporter(t *testing.T) {
	src := strings.NewReader("payload")
	reporter := NewReporter(io.Discard, cli.ProgressAlways, 0, false, time.Now(), false)

	got := NewCountingReader(src, reporter)
	if _, ok := got.(*CountingReader); !ok {
		t.Fatalf("NewCountingReader() type = %T, want *CountingReader", got)
	}
}

func TestCountingReaderReadReportsBytes(t *testing.T) {
	reporter := NewReporter(io.Discard, cli.ProgressAlways, 0, false, time.Now(), false)
	r := &CountingReader{
		reader:   strings.NewReader("hello"),
		reporter: reporter,
	}

	buf := make([]byte, 5)
	n, err := r.Read(buf)
	if err != nil {
		t.Fatalf("Read() error = %v, want nil", err)
	}
	if n != 5 {
		t.Fatalf("Read() n = %d, want 5", n)
	}
	if got := reporter.done.Load(); got != 5 {
		t.Fatalf("done = %d, want 5", got)
	}
}

func TestCountingReaderReadZeroBytesDoesNotReport(t *testing.T) {
	reporter := NewReporter(io.Discard, cli.ProgressAlways, 0, false, time.Now(), false)
	r := &CountingReader{
		reader:   strings.NewReader(""),
		reporter: reporter,
	}

	buf := make([]byte, 8)
	n, err := r.Read(buf)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("Read() error = %v, want io.EOF", err)
	}
	if n != 0 {
		t.Fatalf("Read() n = %d, want 0", n)
	}
	if got := reporter.done.Load(); got != 0 {
		t.Fatalf("done = %d, want 0", got)
	}
}

func TestNewCountingReadCloserWrapsEnabledReporter(t *testing.T) {
	src := io.NopCloser(strings.NewReader("payload"))
	reporter := NewReporter(io.Discard, cli.ProgressAlways, 0, false, time.Now(), false)

	got := NewCountingReadCloser(src, reporter)
	if _, ok := got.(*CountingReadCloser); !ok {
		t.Fatalf("NewCountingReadCloser() type = %T, want *CountingReadCloser", got)
	}
}

func TestCountingReadCloserReadAndClose(t *testing.T) {
	reporter := NewReporter(io.Discard, cli.ProgressAlways, 0, false, time.Now(), false)
	src := &stubReadCloser{reader: strings.NewReader("abc")}
	r := &CountingReadCloser{
		CountingReader: CountingReader{reader: src, reporter: reporter},
		closer:         src,
	}

	buf := make([]byte, 3)
	n, err := r.Read(buf)
	if err != nil {
		t.Fatalf("Read() error = %v, want nil", err)
	}
	if n != 3 {
		t.Fatalf("Read() n = %d, want 3", n)
	}
	if got := reporter.done.Load(); got != 3 {
		t.Fatalf("done = %d, want 3", got)
	}

	if err := r.Close(); err != nil {
		t.Fatalf("Close() error = %v, want nil", err)
	}
	if !src.closed {
		t.Fatalf("Close() did not close wrapped reader")
	}
}

func TestCountingReadCloserCloseReturnsError(t *testing.T) {
	wantErr := errors.New("close failed")
	r := &CountingReadCloser{
		CountingReader: CountingReader{reader: strings.NewReader(""), reporter: &Reporter{enabled: true}},
		closer:         &stubReadCloser{reader: strings.NewReader(""), closeErr: wantErr},
	}

	if err := r.Close(); !errors.Is(err, wantErr) {
		t.Fatalf("Close() error = %v, want %v", err, wantErr)
	}
}

type stubReadCloser struct {
	reader   io.Reader
	closed   bool
	closeErr error
}

func (s *stubReadCloser) Read(p []byte) (int, error) {
	return s.reader.Read(p)
}

func (s *stubReadCloser) Close() error {
	s.closed = true
	return s.closeErr
}
