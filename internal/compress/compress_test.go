package compress

import (
	"bytes"
	"io"
	"strings"
	"testing"

	gzip "github.com/klauspost/pgzip"
)

func TestRoundTrip(t *testing.T) {
	payload := []byte(strings.Repeat("hello-gotgz-", 128))
	cases := []Type{None, Gzip, Bzip2, Xz, Zstd}
	for _, c := range cases {
		t.Run(string(c), func(t *testing.T) {
			var buf bytes.Buffer
			w, err := NewWriter(nopWriteCloser{Writer: &buf}, c)
			if err != nil {
				t.Fatalf("NewWriter() error = %v", err)
			}
			if _, err := w.Write(payload); err != nil {
				t.Fatalf("Write() error = %v", err)
			}
			if err := w.Close(); err != nil {
				t.Fatalf("Close() error = %v", err)
			}

			r, detected, err := NewReader(io.NopCloser(bytes.NewReader(buf.Bytes())), Auto, "archive.tar")
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

func TestDetectByExtension(t *testing.T) {
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	if _, err := gw.Write([]byte("plain")); err != nil {
		t.Fatalf("gzip write error = %v", err)
	}
	if err := gw.Close(); err != nil {
		t.Fatalf("gzip close error = %v", err)
	}
	_, d, err := NewReader(io.NopCloser(bytes.NewReader(buf.Bytes())), Auto, "a.tar.gz")
	if err != nil {
		t.Fatalf("NewReader() error = %v", err)
	}
	if d != Gzip {
		t.Fatalf("detected = %q, want gzip", d)
	}
}

type nopWriteCloser struct{ io.Writer }

func (n nopWriteCloser) Close() error { return nil }
