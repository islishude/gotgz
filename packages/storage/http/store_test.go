package httpstore

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/islishude/gotgz/packages/locator"
)

func TestOpenReaderSuccessWithContentLength(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("method = %q, want %q", r.Method, http.MethodGet)
		}
		_, _ = io.WriteString(w, "archive-bytes")
	}))
	defer server.Close()

	store := &Store{
		client: &http.Client{
			Transport: &http.Transport{DisableCompression: true},
		},
	}
	rc, meta, err := store.OpenReader(context.Background(), locator.Ref{
		Kind: locator.KindHTTP,
		Raw:  server.URL + "/a.tar",
		URL:  server.URL + "/a.tar",
	})
	if err != nil {
		t.Fatalf("OpenReader() error = %v", err)
	}
	defer rc.Close() //nolint:errcheck

	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if string(b) != "archive-bytes" {
		t.Fatalf("body = %q", string(b))
	}
	if meta.Size != int64(len("archive-bytes")) {
		t.Fatalf("size = %d, want %d", meta.Size, len("archive-bytes"))
	}
	if !strings.HasPrefix(meta.ContentType, "text/plain") {
		t.Fatalf("content type = %q", meta.ContentType)
	}
}

func TestOpenReaderSuccessWithoutContentLength(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			t.Fatalf("response writer does not implement http.Flusher")
		}
		_, _ = io.WriteString(w, "chunk-1")
		flusher.Flush()
		_, _ = io.WriteString(w, "chunk-2")
	}))
	defer server.Close()

	store := New()
	rc, meta, err := store.OpenReader(context.Background(), locator.Ref{
		Kind: locator.KindHTTP,
		Raw:  server.URL + "/stream.tar",
		URL:  server.URL + "/stream.tar",
	})
	if err != nil {
		t.Fatalf("OpenReader() error = %v", err)
	}
	defer rc.Close() //nolint:errcheck

	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if string(b) != "chunk-1chunk-2" {
		t.Fatalf("body = %q", string(b))
	}
	if meta.Size != -1 {
		t.Fatalf("size = %d, want -1", meta.Size)
	}
}

func TestOpenReaderGzipContentEncoding(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		var buf bytes.Buffer
		zw := gzip.NewWriter(&buf)
		_, _ = zw.Write([]byte("gzip-body"))
		_ = zw.Close()

		w.Header().Set("Content-Encoding", "gzip")
		w.Header().Set("Content-Type", "application/x-tar")
		_, _ = w.Write(buf.Bytes())
	}))
	defer server.Close()

	store := New()
	rc, meta, err := store.OpenReader(context.Background(), locator.Ref{
		Kind: locator.KindHTTP,
		Raw:  server.URL + "/archive",
		URL:  server.URL + "/archive",
	})
	if err != nil {
		t.Fatalf("OpenReader() error = %v", err)
	}
	defer rc.Close() //nolint:errcheck

	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if string(b) != "gzip-body" {
		t.Fatalf("body = %q", string(b))
	}
	if meta.Size != -1 {
		t.Fatalf("size = %d, want -1", meta.Size)
	}
	if meta.ContentType != "application/x-tar" {
		t.Fatalf("content type = %q, want %q", meta.ContentType, "application/x-tar")
	}
}

func TestOpenReaderNon2xx(t *testing.T) {
	cases := []struct {
		name   string
		code   int
		body   string
		status string
	}{
		{name: "404", code: http.StatusNotFound, body: "not-found", status: "404 Not Found"},
		{name: "500", code: http.StatusInternalServerError, body: "server-error", status: "500 Internal Server Error"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.code)
				_, _ = io.WriteString(w, tc.body)
			}))
			defer server.Close()

			store := New()
			_, _, err := store.OpenReader(context.Background(), locator.Ref{
				Kind: locator.KindHTTP,
				Raw:  server.URL + "/bad.tar",
				URL:  server.URL + "/bad.tar",
			})
			if err == nil {
				t.Fatalf("expected error")
			}
			if !strings.Contains(err.Error(), tc.status) {
				t.Fatalf("error = %q, want status %q", err.Error(), tc.status)
			}
			if !strings.Contains(err.Error(), tc.body) {
				t.Fatalf("error = %q, want body %q", err.Error(), tc.body)
			}
		})
	}
}

func TestOpenReaderRejectsNonHTTPRef(t *testing.T) {
	store := New()
	_, _, err := store.OpenReader(context.Background(), locator.Ref{Kind: locator.KindLocal, Raw: "local.tar"})
	if err == nil {
		t.Fatalf("expected error")
	}
}

func TestOpenRangeReaderSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Range"); got != "bytes=2-5" {
			t.Errorf("Range = %q, want %q", got, "bytes=2-5")
		}
		if got := r.Header.Get("Accept-Encoding"); got != "identity" {
			t.Errorf("Accept-Encoding = %q, want %q", got, "identity")
		}
		w.Header().Set("Content-Range", "bytes 2-5/8")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = io.WriteString(w, "cdef")
	}))
	defer server.Close()

	store := New()
	rc, err := store.OpenRangeReader(context.Background(), locator.Ref{
		Kind: locator.KindHTTP,
		Raw:  server.URL + "/range.zip",
		URL:  server.URL + "/range.zip",
	}, 2, 4)
	if err != nil {
		t.Fatalf("OpenRangeReader() error = %v", err)
	}
	defer rc.Close() //nolint:errcheck

	body, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if string(body) != "cdef" {
		t.Fatalf("body = %q, want %q", body, "cdef")
	}
}

func TestOpenRangeReaderRejectsNonPartialContent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "full-response")
	}))
	defer server.Close()

	store := New()
	_, err := store.OpenRangeReader(context.Background(), locator.Ref{
		Kind: locator.KindHTTP,
		Raw:  server.URL + "/range.zip",
		URL:  server.URL + "/range.zip",
	}, 0, 4)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "200 OK") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestOpenRangeReaderRejectsOverflow(t *testing.T) {
	store := New()
	_, err := store.OpenRangeReader(context.Background(), locator.Ref{
		Kind: locator.KindHTTP,
		Raw:  "https://example.com/range.zip",
		URL:  "https://example.com/range.zip",
	}, math.MaxInt64, 2)
	if err == nil {
		t.Fatal("expected error")
	}
	if got := err.Error(); got != "range end overflows int64 for offset 9223372036854775807 and length 2" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestOpenReaderGzipManualDecode(t *testing.T) {
	// When the client has DisableCompression set, the transport will NOT
	// auto-decompress and Content-Encoding remains in the response. The
	// store must handle gzip manually in that case.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		var buf bytes.Buffer
		zw := gzip.NewWriter(&buf)
		_, _ = zw.Write([]byte("manual-gzip"))
		_ = zw.Close()

		w.Header().Set("Content-Encoding", "gzip")
		w.Header().Set("Content-Type", "application/x-tar")
		_, _ = w.Write(buf.Bytes())
	}))
	defer server.Close()

	store := &Store{
		client: &http.Client{
			Transport: &http.Transport{DisableCompression: true},
		},
	}
	rc, meta, err := store.OpenReader(context.Background(), locator.Ref{
		Kind: locator.KindHTTP,
		Raw:  server.URL + "/manual.tar",
		URL:  server.URL + "/manual.tar",
	})
	if err != nil {
		t.Fatalf("OpenReader() error = %v", err)
	}
	defer rc.Close() //nolint:errcheck

	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if string(b) != "manual-gzip" {
		t.Fatalf("body = %q, want %q", string(b), "manual-gzip")
	}
	if meta.Size != -1 {
		t.Fatalf("size = %d, want -1", meta.Size)
	}
	if meta.ContentType != "application/x-tar" {
		t.Fatalf("content type = %q, want %q", meta.ContentType, "application/x-tar")
	}
}

func TestOpenReaderRejectsUnsupportedContentEncoding(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Encoding", "br")
		_, _ = io.WriteString(w, "encoded-body")
	}))
	defer server.Close()

	store := &Store{
		client: &http.Client{
			Transport: &http.Transport{DisableCompression: true},
		},
	}
	_, _, err := store.OpenReader(context.Background(), locator.Ref{
		Kind: locator.KindHTTP,
		Raw:  server.URL + "/encoded.tar",
		URL:  server.URL + "/encoded.tar",
	})
	if err == nil {
		t.Fatalf("expected error")
	}
	if !strings.Contains(err.Error(), "unsupported http content-encoding") {
		t.Fatalf("unexpected error: %v", err)
	}
}
