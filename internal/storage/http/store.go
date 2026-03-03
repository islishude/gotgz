package httpstore

import (
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
	Size int64
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
	return resp.Body, Metadata{Size: resp.ContentLength}, nil
}
