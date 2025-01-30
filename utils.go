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

func IsSymbolicLink(mode os.FileMode) bool {
	return mode&os.ModeSymlink != 0
}

func StripComponents(p string, n int) string {
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

func AddTarSuffix(fileName, suffix string) string {
	if suffix == "" {
		return fileName
	}
	ext := filepath.Ext(fileName)
	// don't add suffix if the file is a hidden name
	if ext == fileName {
		return fileName
	}
	dir := filepath.Dir(fileName)
	if strings.HasSuffix(fileName, ".tar"+ext) {
		ext = ".tar" + ext
	}
	file := strings.TrimSuffix(filepath.Base(fileName), ext)
	switch suffix {
	case "date":
		file = fmt.Sprintf("%s-%s%s", file, time.Now().Format("20060102"), ext)
	default:
		file = fmt.Sprintf("%s-%s%s", file, suffix, ext)
	}
	return filepath.Join(dir, file)
}
