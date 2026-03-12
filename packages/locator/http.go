package locator

import (
	"fmt"
	"net/url"
	"strings"
)

// parseHTTPURI parses an HTTP(S) archive URI and validates its basic shape.
func parseHTTPURI(v string) (Ref, error) {
	u, err := url.Parse(v)
	if err != nil {
		return Ref{}, fmt.Errorf("invalid http uri %q: %w", v, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return Ref{}, fmt.Errorf("unsupported uri scheme %q", u.Scheme)
	}
	if strings.TrimSpace(u.Host) == "" {
		return Ref{}, fmt.Errorf("http uri must include host")
	}
	return Ref{Kind: KindHTTP, Raw: v, URL: v}, nil
}
