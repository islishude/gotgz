package httpstore

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/islishude/gotgz/internal/locator"
)

func TestOpenReaderSuccessWithContentLength(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("method = %q, want %q", r.Method, http.MethodGet)
		}
		_, _ = io.WriteString(w, "archive-bytes")
	}))
	defer server.Close()

	store := New()
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
