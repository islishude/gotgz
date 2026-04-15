package engine

import (
	"archive/zip"
	"fmt"
	"io"
	"math"
	"os"
	"strings"

	"github.com/islishude/gotgz/packages/archiveprogress"
)

// totalZipPayloadBytes sums uncompressed payload bytes for matching entries.
func totalZipPayloadBytes(zr *zip.Reader, match func(zf *zip.File) bool) int64 {
	var total uint64
	for _, zf := range zr.File {
		if match != nil && !match(zf) {
			continue
		}
		if isZipDir(zf) {
			continue
		}
		total += zf.UncompressedSize64
	}
	if total > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(total)
}

// isZipDir reports whether a zip entry is a directory.
func isZipDir(zf *zip.File) bool {
	if zf == nil {
		return false
	}
	if strings.HasSuffix(zf.Name, "/") {
		return true
	}
	return zf.FileInfo().IsDir()
}

// isZipSymlink reports whether a zip entry is a symbolic link.
func isZipSymlink(zf *zip.File) bool {
	if zf == nil {
		return false
	}
	return zf.Mode()&os.ModeSymlink != 0
}

// isZipRegular reports whether a zip entry should be treated as a regular file.
func isZipRegular(zf *zip.File) bool {
	if zf == nil || isZipDir(zf) || isZipSymlink(zf) {
		return false
	}
	return zf.Mode().IsRegular()
}

// readZipSymlinkTarget reads a symlink target from a zip entry with a hard cap
// to avoid unbounded memory growth on malformed archives.
func readZipSymlinkTarget(zf *zip.File, rc io.Reader, reporter *archiveprogress.Reporter) (string, error) {
	b, err := io.ReadAll(io.LimitReader(archiveprogress.NewCountingReader(rc, reporter), maxZipSymlinkTargetBytes+1))
	if err != nil {
		return "", err
	}
	if len(b) > maxZipSymlinkTargetBytes {
		return "", fmt.Errorf("zip symlink %s target exceeds %d bytes", zf.Name, maxZipSymlinkTargetBytes)
	}
	return string(b), nil
}
