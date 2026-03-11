package httpstore

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"strings"

	"github.com/islishude/gotgz/packages/locator"
)

const maxErrorBodyBytes = 4 * 1024

// Store reads archive objects from HTTP(S) sources.
type Store struct {
	client *http.Client
}

// Metadata carries basic metadata for an HTTP archive response.
type Metadata struct {
	Size        int64
	ContentType string
}

// New creates a Store backed by the default HTTP client.
func New() *Store {
	return &Store{client: http.DefaultClient}
}

// OpenReader opens an HTTP archive source and returns the response body stream.
func (s *Store) OpenReader(ctx context.Context, ref locator.Ref) (io.ReadCloser, Metadata, error) {
	if ref.Kind != locator.KindHTTP {
		return nil, Metadata{}, fmt.Errorf("ref %q is not http", ref.Raw)
	}
	client := s.client
	if client == nil {
		client = http.DefaultClient
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ref.URL, nil)
	if err != nil {
		return nil, Metadata{}, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, Metadata{}, err
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		msg := fmt.Sprintf("http GET %q failed: status %s", ref.URL, resp.Status)
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, maxErrorBodyBytes))
		_ = resp.Body.Close()
		bodyText := strings.TrimSpace(string(body))
		if readErr != nil {
			return nil, Metadata{}, fmt.Errorf("%s (and reading error body: %w)", msg, readErr)
		}
		if bodyText != "" {
			return nil, Metadata{}, fmt.Errorf("%s: %s", msg, bodyText)
		}
		return nil, Metadata{}, fmt.Errorf("%s", msg)
	}

	body, size, err := decodeResponseBody(resp)
	if err != nil {
		return nil, Metadata{}, err
	}
	meta := Metadata{
		Size:        size,
		ContentType: strings.TrimSpace(resp.Header.Get("Content-Type")),
	}
	return body, meta, nil
}

// OpenRangeReader opens one byte range from an HTTP(S) archive source.
func (s *Store) OpenRangeReader(ctx context.Context, ref locator.Ref, offset int64, length int64) (io.ReadCloser, error) {
	if ref.Kind != locator.KindHTTP {
		return nil, fmt.Errorf("ref %q is not http", ref.Raw)
	}
	if offset < 0 {
		return nil, fmt.Errorf("range offset must be >= 0")
	}
	if length < 0 {
		return nil, fmt.Errorf("range length must be >= 0")
	}
	if length == 0 {
		return io.NopCloser(bytes.NewReader(nil)), nil
	}
	if offset > math.MaxInt64-(length-1) {
		return nil, fmt.Errorf("range end overflows int64 for offset %d and length %d", offset, length)
	}

	client := s.client
	if client == nil {
		client = http.DefaultClient
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ref.URL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, offset+length-1))
	req.Header.Set("Accept-Encoding", "identity")

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusPartialContent {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBodyBytes))
		_ = resp.Body.Close()
		text := strings.TrimSpace(string(body))
		if text != "" {
			return nil, fmt.Errorf("http range GET %q failed: status %s: %s", ref.URL, resp.Status, text)
		}
		return nil, fmt.Errorf("http range GET %q failed: status %s", ref.URL, resp.Status)
	}
	if encoding := strings.TrimSpace(strings.ToLower(resp.Header.Get("Content-Encoding"))); encoding != "" && encoding != "identity" {
		_ = resp.Body.Close()
		return nil, fmt.Errorf("http range GET %q returned unsupported content-encoding %q", ref.URL, encoding)
	}
	return resp.Body, nil
}

// decodeResponseBody returns the response body and its content size.
//
// When the standard http.Transport has already transparently decompressed the
// body (resp.Uncompressed == true), the decompressed size is unknown so -1 is
// returned. When the transport did NOT decompress (e.g. DisableCompression is
// set), the function inspects Content-Encoding and handles gzip/x-gzip
// manually; unsupported encodings result in an error.
func decodeResponseBody(resp *http.Response) (io.ReadCloser, int64, error) {
	// The Go http.Transport transparently decompresses gzip responses when
	// the client did not explicitly set Accept-Encoding. In that case
	// resp.Uncompressed is true, Content-Encoding is stripped, and
	// ContentLength no longer reflects the actual (decompressed) size.
	if resp.Uncompressed {
		return resp.Body, -1, nil
	}

	// Transport did not decompress; honour Content-Encoding ourselves.
	contentEncoding := strings.TrimSpace(strings.ToLower(resp.Header.Get("Content-Encoding")))

	switch contentEncoding {
	case "", "identity":
		return resp.Body, resp.ContentLength, nil
	case "gzip", "x-gzip":
		zr, err := gzip.NewReader(resp.Body)
		if err != nil {
			_ = resp.Body.Close()
			return nil, 0, fmt.Errorf("gzip content-encoding: %w", err)
		}
		// Decompressed size is unknown up front.
		return &multiReadCloser{reader: zr, closers: []io.Closer{zr, resp.Body}}, -1, nil
	default:
		_ = resp.Body.Close()
		return nil, 0, fmt.Errorf("unsupported http content-encoding %q for archive source", contentEncoding)
	}
}

// multiReadCloser wraps a reader with multiple closers so that closing
// cascades through all layers (e.g. gzip reader + underlying body).
type multiReadCloser struct {
	reader  io.Reader
	closers []io.Closer
}

// Read forwards reads to the wrapped reader.
func (m *multiReadCloser) Read(p []byte) (int, error) { return m.reader.Read(p) }

// Close closes all wrapped closers and returns the first close error.
func (m *multiReadCloser) Close() error {
	var first error
	for _, c := range m.closers {
		if err := c.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}
