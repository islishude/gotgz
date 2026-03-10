package compress

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/pierrec/lz4/v4"
)

// closeRecorder records close order and can inject a close error.
type closeRecorder struct {
	name     string
	events   *[]string
	closeErr error
}

// Close records the close operation and returns the configured error.
func (r *closeRecorder) Close() error {
	*r.events = append(*r.events, "close:"+r.name)
	return r.closeErr
}

// writeCloserRecorder records writes, flushes, and closes for stacked writer tests.
type writeCloserRecorder struct {
	name     string
	events   *[]string
	buf      bytes.Buffer
	writeErr error
	closeErr error
	flushErr error
}

// Write records the write operation and appends the payload to the internal buffer.
func (r *writeCloserRecorder) Write(p []byte) (int, error) {
	*r.events = append(*r.events, "write:"+r.name)
	if r.writeErr != nil {
		return 0, r.writeErr
	}
	return r.buf.Write(p)
}

// Close records the close operation and returns the configured error.
func (r *writeCloserRecorder) Close() error {
	*r.events = append(*r.events, "close:"+r.name)
	return r.closeErr
}

// Flush records the flush operation and returns the configured error.
func (r *writeCloserRecorder) Flush() error {
	*r.events = append(*r.events, "flush:"+r.name)
	return r.flushErr
}

// TestFromString verifies that supported names map to concrete compression types
// and unknown values fall back to auto detection.
func TestFromString(t *testing.T) {
	tests := []struct {
		input string
		want  Type
	}{
		{input: "none", want: None},
		{input: "GZIP", want: Gzip},
		{input: "bzip2", want: Bzip2},
		{input: "xz", want: Xz},
		{input: "zstd", want: Zstd},
		{input: "lz4", want: Lz4},
		{input: "unknown", want: Auto},
	}

	for _, tt := range tests {
		if got := FromString(tt.input); got != tt.want {
			t.Fatalf("FromString(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

// TestNewWriterRejectsInvalidOptions verifies that invalid levels and unknown
// compression types are rejected before writing any data.
func TestNewWriterRejectsInvalidOptions(t *testing.T) {
	level := 10
	if _, err := NewWriter(nopWriteCloser{Writer: io.Discard}, Gzip, WriterOptions{Level: &level}); err == nil {
		t.Fatalf("NewWriter() error = nil, want invalid level error")
	}

	if _, err := NewWriter(nopWriteCloser{Writer: io.Discard}, Type("brotli"), WriterOptions{}); err == nil {
		t.Fatalf("NewWriter() error = nil, want unsupported type error")
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

// TestStackedWriteCloserFlushAndClose verifies flush forwarding and writer-first
// close ordering for stacked compression writers.
func TestStackedWriteCloserFlushAndClose(t *testing.T) {
	events := make([]string, 0, 4)
	writer := &writeCloserRecorder{name: "writer", events: &events}
	dst := &closeRecorder{name: "dst", events: &events}
	w := &stackedWriteCloser{writer: writer, dst: dst, closeWriterFirst: true}

	if _, err := w.Write([]byte("payload")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	wantEvents := []string{"write:writer", "flush:writer", "close:writer", "close:dst"}
	if got := strings.Join(events, ","); got != strings.Join(wantEvents, ",") {
		t.Fatalf("events = %#v, want %#v", events, wantEvents)
	}
	if got := writer.buf.String(); got != "payload" {
		t.Fatalf("writer payload = %q, want %q", got, "payload")
	}
}

// TestStackedWriteCloserCloseDestinationFirst verifies the alternate close
// order used by stacked writers when the destination must close first.
func TestStackedWriteCloserCloseDestinationFirst(t *testing.T) {
	events := make([]string, 0, 2)
	writer := &writeCloserRecorder{name: "writer", events: &events}
	dst := &closeRecorder{name: "dst", events: &events}
	w := &stackedWriteCloser{writer: writer, dst: dst, closeWriterFirst: false}

	if err := w.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	wantEvents := []string{"close:dst", "close:writer"}
	if got := strings.Join(events, ","); got != strings.Join(wantEvents, ",") {
		t.Fatalf("events = %#v, want %#v", events, wantEvents)
	}
}

// TestPlainWriteCloserFlushAndClose verifies that plain writers expose a no-op
// Flush and delegate Close to the destination writer.
func TestPlainWriteCloserFlushAndClose(t *testing.T) {
	events := make([]string, 0, 2)
	dst := &writeCloserRecorder{name: "plain", events: &events}
	w := &plainWriteCloser{dst: dst}

	if _, err := w.Write([]byte("payload")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if got := dst.buf.String(); got != "payload" {
		t.Fatalf("plain payload = %q, want %q", got, "payload")
	}
}

// TestNormalizeLevel verifies nil, valid, and invalid compression levels.
func TestNormalizeLevel(t *testing.T) {
	if got, ok, err := normalizeLevel(nil); got != 0 || ok || err != nil {
		t.Fatalf("normalizeLevel(nil) = (%d, %t, %v), want (0, false, nil)", got, ok, err)
	}

	level := 6
	if got, ok, err := normalizeLevel(&level); got != 6 || !ok || err != nil {
		t.Fatalf("normalizeLevel(valid) = (%d, %t, %v), want (6, true, nil)", got, ok, err)
	}

	level = 0
	if _, _, err := normalizeLevel(&level); err == nil {
		t.Fatalf("normalizeLevel(invalid) error = nil, want non-nil")
	}
}

// TestXZDictCapForLevel verifies representative xz preset mappings.
func TestXZDictCapForLevel(t *testing.T) {
	tests := []struct {
		level int
		want  int
	}{
		{level: 1, want: 256 << 10},
		{level: 4, want: 4 << 20},
		{level: 6, want: 8 << 20},
		{level: 9, want: 32 << 20},
	}

	for _, tt := range tests {
		if got := xzDictCapForLevel(tt.level); got != tt.want {
			t.Fatalf("xzDictCapForLevel(%d) = %d, want %d", tt.level, got, tt.want)
		}
	}
}

// TestLz4Level verifies representative lz4 level mappings.
func TestLz4Level(t *testing.T) {
	tests := []struct {
		level int
		want  lz4.CompressionLevel
	}{
		{level: 1, want: lz4.Level1},
		{level: 5, want: lz4.Level5},
		{level: 8, want: lz4.Level8},
		{level: 9, want: lz4.Level9},
	}

	for _, tt := range tests {
		got := lz4Level(tt.level)
		if got != tt.want {
			t.Fatalf("lz4Level(%d) = %v, want %v", tt.level, got, tt.want)
		}
	}
}
