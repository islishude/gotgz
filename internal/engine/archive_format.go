package engine

import (
	"net/url"
	"strings"

	"github.com/islishude/gotgz/internal/locator"
)

type archiveFormat string

const (
	archiveFormatTar archiveFormat = "tar"
	archiveFormatZip archiveFormat = "zip"
)

// detectCreateArchiveFormat resolves output archive format from destination hint.
func detectCreateArchiveFormat(ref locator.Ref) archiveFormat {
	if hasZipHint(archiveNameHint(ref)) {
		return archiveFormatZip
	}
	return archiveFormatTar
}

// detectReadArchiveFormat resolves input archive format by magic, then hint,
// then content type.
func detectReadArchiveFormat(magic []byte, hint string, contentType string) archiveFormat {
	if isZipMagic(magic) {
		return archiveFormatZip
	}
	if hasZipHint(hint) {
		return archiveFormatZip
	}
	if isZipContentType(contentType) {
		return archiveFormatZip
	}
	return archiveFormatTar
}

// archiveNameHint returns the best-effort filename/path hint for format detection.
func archiveNameHint(ref locator.Ref) string {
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

// hasZipHint reports whether the provided path-like hint implies zip format.
func hasZipHint(v string) bool {
	hint := strings.TrimSpace(v)
	if hint == "" {
		return false
	}
	if u, err := url.Parse(hint); err == nil && u.Scheme != "" {
		hint = u.Path
	}
	return strings.HasSuffix(strings.ToLower(hint), ".zip")
}

// isZipMagic reports whether the provided bytes match ZIP local/central signatures.
func isZipMagic(magic []byte) bool {
	if len(magic) < 4 {
		return false
	}
	return (magic[0] == 'P' && magic[1] == 'K' && magic[2] == 0x03 && magic[3] == 0x04) ||
		(magic[0] == 'P' && magic[1] == 'K' && magic[2] == 0x05 && magic[3] == 0x06) ||
		(magic[0] == 'P' && magic[1] == 'K' && magic[2] == 0x07 && magic[3] == 0x08)
}

// isZipContentType reports whether HTTP/S3 content type implies zip format.
func isZipContentType(v string) bool {
	ct := strings.ToLower(strings.TrimSpace(v))
	if ct == "" {
		return false
	}
	mediaType, _, _ := strings.Cut(ct, ";")
	mediaType = strings.TrimSpace(mediaType)
	return mediaType == "application/zip" || mediaType == "application/x-zip-compressed"
}
