package gotgz

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type Logger interface {
	Error(msg string, args ...any)
	Debug(msg string, args ...any)
	Warn(msg string, args ...any)
	Info(msg string, args ...any)
}

func isPathInvalid(p string) bool {
	return p == "" || strings.Contains(p, `\`) || strings.Contains(p, "../") || strings.HasPrefix(p, "/")
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

func GetCompressionHandlers(alg string) (Archiver, error) {
	parsed, err := url.Parse(alg)
	if err != nil {
		return nil, err
	}

	query, err := url.ParseQuery(parsed.RawQuery)
	if err != nil {
		return nil, err
	}

	switch parsed.Path {
	case "gzip", "gz":
		return NewGZip(query)
	case "lz4":
		return NewLz4(query)
	case "zstd":
		return NewZstd(query)
	default:
		return nil, fmt.Errorf("unsupported compression algorithm: %s", alg)
	}
}

func AddFileSuffix(fileName, suffix string) string {
	if suffix == "" {
		return fileName
	}
	dir, ext := filepath.Dir(fileName), filepath.Ext(fileName)
	file := strings.TrimSuffix(filepath.Base(fileName), ext)
	switch suffix {
	case "date":
		file = fmt.Sprintf("%s-%s%s", file, time.Now().Format("20060102"), ext)
	}
	return filepath.Join(dir, file)
}
