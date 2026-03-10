package archiveutil

import (
	"net/url"
	"strings"

	"github.com/islishude/gotgz/internal/locator"
)

// NameHint returns the best-effort filename/path hint for archive format detection.
func NameHint(ref locator.Ref) string {
	switch ref.Kind {
	case locator.KindS3:
		return ref.Key
	case locator.KindHTTP:
		u, err := url.Parse(ref.URL)
		if err != nil {
			return ref.URL
		}
		return u.Path
	case locator.KindLocal:
		return ref.Path
	default:
		return ref.Raw
	}
}

// HasZipHint reports whether the provided path-like hint implies zip format.
func HasZipHint(v string) bool {
	hint := strings.TrimSpace(v)
	if hint == "" {
		return false
	}
	if u, err := url.Parse(hint); err == nil && u.Scheme != "" {
		hint = u.Path
	}
	return strings.HasSuffix(strings.ToLower(hint), ".zip")
}
