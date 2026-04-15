package compress

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/pierrec/lz4/v4"
)

func TestRoundTrip(t *testing.T) {
	payload := []byte(strings.Repeat("hello-gotgz-", 128))
	cases := []Type{None, Gzip, Bzip2, Xz, Zstd, Lz4}
	for _, c := range cases {
		t.Run(string(c), func(t *testing.T) {
			var buf bytes.Buffer
			w, err := NewWriter(nopWriteCloser{Writer: &buf}, c, WriterOptions{})
			if err != nil {
				t.Fatalf("NewWriter() error = %v", err)
			}
			if _, err := w.Write(payload); err != nil {
				t.Fatalf("Write() error = %v", err)
			}
			if err := w.Close(); err != nil {
				t.Fatalf("Close() error = %v", err)
			}

			r, detected, err := NewReader(io.NopCloser(bytes.NewReader(buf.Bytes())), Auto, "archive.tar", "")
			if err != nil {
				t.Fatalf("NewReader() error = %v", err)
			}
			got, err := io.ReadAll(r)
			if err != nil {
				t.Fatalf("ReadAll() error = %v", err)
			}
			_ = r.Close()
			if !bytes.Equal(got, payload) {
				t.Fatalf("payload mismatch")
			}
			if c == None && detected != None {
				t.Fatalf("detected = %q, want none", detected)
			}
		})
	}
}

func TestRoundTripWithCompressionLevel(t *testing.T) {
	payload := []byte(strings.Repeat("hello-gotgz-level-", 128))
	level := 9
	cases := []Type{Gzip, Bzip2, Xz, Zstd, Lz4}
	for _, c := range cases {
		t.Run(string(c), func(t *testing.T) {
			var buf bytes.Buffer
			w, err := NewWriter(nopWriteCloser{Writer: &buf}, c, WriterOptions{Level: &level})
			if err != nil {
				t.Fatalf("NewWriter() error = %v", err)
			}
			if _, err := w.Write(payload); err != nil {
				t.Fatalf("Write() error = %v", err)
			}
			if err := w.Close(); err != nil {
				t.Fatalf("Close() error = %v", err)
			}

			r, _, err := NewReader(io.NopCloser(bytes.NewReader(buf.Bytes())), c, "archive.tar", "")
			if err != nil {
				t.Fatalf("NewReader() error = %v", err)
			}
			got, err := io.ReadAll(r)
			if err != nil {
				t.Fatalf("ReadAll() error = %v", err)
			}
			_ = r.Close()
			if !bytes.Equal(got, payload) {
				t.Fatalf("payload mismatch")
			}
		})
	}
}

func TestBzip2WriterFlushEmitsData(t *testing.T) {
	var buf bytes.Buffer
	w, err := NewWriter(nopWriteCloser{Writer: &buf}, Bzip2, WriterOptions{})
	if err != nil {
		t.Fatalf("NewWriter() error = %v", err)
	}

	flusher, ok := w.(FlushWriteCloser)
	if !ok {
		t.Fatalf("NewWriter(Bzip2) does not implement FlushWriteCloser")
	}

	if _, err := w.Write([]byte("flush-me")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if err := flusher.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	if buf.Len() == 0 {
		t.Fatalf("Flush() did not emit any compressed bytes")
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	r, _, err := NewReader(io.NopCloser(bytes.NewReader(buf.Bytes())), Bzip2, "archive.tar.bz2", "")
	if err != nil {
		t.Fatalf("NewReader() error = %v", err)
	}
	defer func() {
		if err := r.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	}()

	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if !bytes.Equal(got, []byte("flush-me")) {
		t.Fatalf("payload = %q, want %q", got, "flush-me")
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
