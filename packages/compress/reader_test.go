package compress

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"

	gzip "github.com/klauspost/pgzip"
)

func TestExplicitCompressionMismatch(t *testing.T) {
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	if _, err := gw.Write([]byte("plain")); err != nil {
		t.Fatalf("gzip write error = %v", err)
	}
	if err := gw.Close(); err != nil {
		t.Fatalf("gzip close error = %v", err)
	}
	_, _, err := NewReader(io.NopCloser(bytes.NewReader(buf.Bytes())), Zstd, "a.tar.gz", "application/gzip")
	if err == nil {
		t.Fatalf("expected mismatch error")
	}
	if !strings.Contains(err.Error(), "does not match archive data") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNewReaderAllowsShortProbe(t *testing.T) {
	payload := []byte("plain")
	r, detected, err := NewReader(io.NopCloser(bytes.NewReader(payload)), Auto, "archive.tar", "")
	if err != nil {
		t.Fatalf("NewReader() error = %v", err)
	}
	if detected != None {
		t.Fatalf("detected = %q, want none", detected)
	}
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("payload mismatch")
	}
}

func TestNewReaderReturnsProbeError(t *testing.T) {
	probeErr := io.ErrClosedPipe
	_, detected, err := NewReader(&errReadCloser{err: probeErr}, Auto, "archive.tar", "")
	if err == nil {
		t.Fatalf("expected probe error")
	}
	if detected != Auto {
		t.Fatalf("detected = %q, want auto", detected)
	}
	if err != probeErr {
		t.Fatalf("error = %v, want %v", err, probeErr)
	}
}

// TestNewReaderFallsBackToContentType verifies that content-type detection is
// used when magic bytes and hint suffixes are inconclusive.
func TestNewReaderFallsBackToContentType(t *testing.T) {
	reader, detected, err := NewReader(io.NopCloser(strings.NewReader("plain payload")), Auto, "archive.bin", "application/x-tar")
	if err != nil {
		t.Fatalf("NewReader() error = %v", err)
	}
	defer func() {
		if err := reader.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	}()

	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if string(got) != "plain payload" {
		t.Fatalf("ReadAll() = %q, want %q", got, "plain payload")
	}
	if detected != None {
		t.Fatalf("detected = %q, want %q", detected, None)
	}
}

// TestWrapReaderRejectsUnsupportedType verifies that wrapReader fails fast on
// unsupported compression values.
func TestWrapReaderRejectsUnsupportedType(t *testing.T) {
	if _, err := wrapReader(strings.NewReader("payload"), io.NopCloser(strings.NewReader("")), Type("brotli")); err == nil {
		t.Fatalf("wrapReader() error = nil, want unsupported type error")
	}
}

// TestMultiReadCloserCloseReturnsFirstError verifies that all closers run and
// the first close error is returned.
func TestMultiReadCloserCloseReturnsFirstError(t *testing.T) {
	events := make([]string, 0, 2)
	wantErr := errors.New("first close failed")
	reader := &multiReadCloser{
		reader: strings.NewReader("payload"),
		closers: []io.Closer{
			&closeRecorder{name: "first", events: &events, closeErr: wantErr},
			&closeRecorder{name: "second", events: &events},
		},
	}

	if err := reader.Close(); !errors.Is(err, wantErr) {
		t.Fatalf("Close() error = %v, want %v", err, wantErr)
	}
	wantEvents := []string{"close:first", "close:second"}
	if !bytes.Equal([]byte(strings.Join(events, ",")), []byte(strings.Join(wantEvents, ","))) {
		t.Fatalf("close events = %#v, want %#v", events, wantEvents)
	}
}
