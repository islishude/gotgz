package archiveutil

import (
	"mime"
	"path/filepath"
	"strings"
)

// ContentTypeForKey infers archive content type from an object key suffix.
func ContentTypeForKey(key string) string {
	v := strings.ToLower(strings.TrimSpace(key))
	switch {
	case strings.HasSuffix(v, ".tar.gz"), strings.HasSuffix(v, ".tgz"), strings.HasSuffix(v, ".gz"):
		return "application/gzip"
	case strings.HasSuffix(v, ".tar.bz2"), strings.HasSuffix(v, ".tbz2"), strings.HasSuffix(v, ".tbz"), strings.HasSuffix(v, ".bz2"):
		return "application/x-bzip2"
	case strings.HasSuffix(v, ".tar.xz"), strings.HasSuffix(v, ".txz"), strings.HasSuffix(v, ".xz"):
		return "application/x-xz"
	case strings.HasSuffix(v, ".tar.zst"), strings.HasSuffix(v, ".tzst"), strings.HasSuffix(v, ".zstd"), strings.HasSuffix(v, ".zst"):
		return "application/zstd"
	case strings.HasSuffix(v, ".tar.lz4"), strings.HasSuffix(v, ".tlz4"), strings.HasSuffix(v, ".lz4"):
		return "application/x-lz4"
	case strings.HasSuffix(v, ".zip"):
		return "application/zip"
	case strings.HasSuffix(v, ".tar"), strings.HasSuffix(v, ".tape"):
		return "application/x-tar"
	}
	ext := filepath.Ext(v)
	if ext == "" {
		return "application/octet-stream"
	}
	return mime.TypeByExtension(ext)
}
