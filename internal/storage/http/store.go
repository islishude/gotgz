package httpstore

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/islishude/gotgz/internal/locator"
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
	return body, Metadata{Size: size, ContentType: strings.TrimSpace(resp.Header.Get("Content-Type"))}, nil
}

// decodeResponseBody handles transport-level content encoding for archive streams.
func decodeResponseBody(resp *http.Response) (io.ReadCloser, int64, error) {
	contentEncoding := strings.TrimSpace(strings.ToLower(resp.Header.Get("Content-Encoding")))
	size := resp.ContentLength

	switch contentEncoding {
	case "", "identity":
		return resp.Body, size, nil
	case "gzip", "x-gzip":
		zr, err := gzip.NewReader(resp.Body)
		if err != nil {
			_ = resp.Body.Close()
			return nil, 0, fmt.Errorf("unsupported http content-encoding %q: %w", contentEncoding, err)
		}
		// Decompressed size is unknown up front.
		return &multiReadCloser{reader: zr, closers: []io.Closer{zr, resp.Body}}, -1, nil
	default:
		_ = resp.Body.Close()
		return nil, 0, fmt.Errorf("unsupported http content-encoding %q for archive source", contentEncoding)
	}
}

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
