package gotgz

import (
	"compress/gzip"
	"fmt"
	"io"
	"net/url"
	"os"
	"strconv"
	"strings"
)

func debugf(debug bool, l string, p ...interface{}) {
	if debug {
		fmt.Printf(l, p...)
		fmt.Println()
	}
}

func isPathInvalid(p string) bool {
	return p == "" || strings.Contains(p, `\`) || strings.Contains(p, "../")
}

func isSymbolicLink(mode os.FileMode) bool {
	return mode&os.ModeSymlink != 0
}

func stripComponents(p string, n int) string {
	if n <= 0 {
		return p
	}

	parts := strings.Split(p, "/")
	// if n is greater than the number of parts, return the last part
	if n >= len(parts) {
		n = len(parts) - 1
	}

	return strings.Join(parts[n:], "/")
}

func IsS3(u *url.URL) bool {
	return u.Scheme == "s3"
}

func ParseMetadata(raw string) (map[string]string, error) {
	if raw == "" {
		return nil, nil
	}

	// parse http query from meta string
	val, err := url.ParseQuery(raw)
	if err != nil {
		return nil, err
	}

	meta := make(map[string]string, len(val))
	for k, v := range val {
		if len(v) > 1 {
			return nil, fmt.Errorf("metadata key %s has multiple values", k)
		}
		if v[0] == "" {
			return nil, fmt.Errorf("metadata key %s has no value", k)
		}
		meta[k] = v[0]
	}
	return meta, nil
}

func GetCompressionHandlers(alg string) (ZipWriter, ZipReader, string, error) {
	parsed, err := url.Parse(alg)
	if err != nil {
		return nil, nil, "", err
	}

	query, err := url.ParseQuery(parsed.RawQuery)
	if err != nil {
		return nil, nil, "", err
	}

	switch parsed.Path {
	case "gzip", "gz":
		var level = gzip.DefaultCompression
		if levelQuery := query.Get("level"); levelQuery != "" {
			level, err = strconv.Atoi(levelQuery)
			if err != nil {
				return nil, nil, "", err
			}
		}
		return func(buf io.WriteCloser) (io.WriteCloser, error) {
				return gzip.NewWriterLevel(buf, level)
			},
			func(src io.ReadCloser) (io.Reader, error) {
				return gzip.NewReader(src)
			},
			"application/x-gzip", nil
	default:
		return nil, nil, "", fmt.Errorf("unsupported compression algorithm: %s", alg)
	}
}
