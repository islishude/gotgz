package httpstore

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/islishude/gotgz/packages/archiveutil"
	"github.com/islishude/gotgz/packages/locator"
)

const maxErrorBodyBytes = 4 * 1024

// Store reads archive objects from HTTP(S) sources.
type Store struct {
	client *http.Client
}

// Metadata carries basic metadata for an HTTP archive response.
type Metadata struct {
	Size         int64
	ContentType  string
	ETag         string
	LastModified string
	URL          string
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
		Size:         size,
		ContentType:  strings.TrimSpace(resp.Header.Get("Content-Type")),
		ETag:         strings.TrimSpace(resp.Header.Get("ETag")),
		LastModified: strings.TrimSpace(resp.Header.Get("Last-Modified")),
	}
	if resp.Request != nil && resp.Request.URL != nil {
		meta.URL = resp.Request.URL.String()
	}
	return body, meta, nil
}

// OpenRangeReader opens one byte range from an HTTP(S) archive source.
func (s *Store) OpenRangeReader(ctx context.Context, ref locator.Ref, offset int64, length int64) (io.ReadCloser, error) {
	return s.OpenRangeReaderSnapshot(ctx, ref, offset, length, archiveutil.Snapshot{})
}

// OpenRangeReaderSnapshot opens a byte range fenced to the initial HTTP
// response validator and verifies the exact Content-Range returned.
func (s *Store) OpenRangeReaderSnapshot(ctx context.Context, ref locator.Ref, offset int64, length int64, snapshot archiveutil.Snapshot) (io.ReadCloser, error) {
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

	requestURL := ref.URL
	if snapshot.URL != "" {
		if _, err := url.ParseRequestURI(snapshot.URL); err != nil {
			return nil, fmt.Errorf("invalid snapshot URL %q: %w", snapshot.URL, err)
		}
		requestURL = snapshot.URL
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, offset+length-1))
	req.Header.Set("Accept-Encoding", "identity")
	if etag := strings.TrimSpace(snapshot.ETag); etag != "" && !strings.HasPrefix(etag, "W/") {
		req.Header.Set("If-Range", etag)
	} else if modified := strings.TrimSpace(snapshot.LastModified); modified != "" {
		req.Header.Set("If-Range", modified)
	}

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
	if err := validateContentRange(resp, offset, length, snapshot.Size); err != nil {
		_ = resp.Body.Close()
		return nil, err
	}
	return resp.Body, nil
}

func validateContentRange(resp *http.Response, offset, length, expectedTotal int64) error {
	header := strings.TrimSpace(resp.Header.Get("Content-Range"))
	rawRange, rawTotal, ok := strings.Cut(strings.TrimPrefix(header, "bytes "), "/")
	if !ok || !strings.HasPrefix(header, "bytes ") {
		return fmt.Errorf("invalid Content-Range %q", header)
	}
	rawStart, rawEnd, ok := strings.Cut(rawRange, "-")
	if !ok {
		return fmt.Errorf("invalid Content-Range %q", header)
	}
	start, startErr := strconv.ParseInt(rawStart, 10, 64)
	end, endErr := strconv.ParseInt(rawEnd, 10, 64)
	total, totalErr := strconv.ParseInt(rawTotal, 10, 64)
	if startErr != nil || endErr != nil || totalErr != nil || start < 0 || end < start || total <= end {
		return fmt.Errorf("invalid Content-Range %q", header)
	}
	if start != offset || end != offset+length-1 {
		return fmt.Errorf("unexpected Content-Range %q for requested bytes %d-%d", header, offset, offset+length-1)
	}
	if expectedTotal > 0 && total != expectedTotal {
		return fmt.Errorf("unexpected Content-Range total %d, want %d", total, expectedTotal)
	}
	if resp.ContentLength >= 0 && resp.ContentLength != length {
		return fmt.Errorf("unexpected HTTP range length %d, want %d", resp.ContentLength, length)
	}
	return nil
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
