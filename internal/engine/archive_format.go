package engine

import (
	"strings"

	"github.com/islishude/gotgz/internal/archiveutil"
	"github.com/islishude/gotgz/internal/locator"
)

type archiveFormat string

const (
	archiveFormatTar archiveFormat = "tar"
	archiveFormatZip archiveFormat = "zip"
)

// detectCreateArchiveFormat resolves output archive format from destination hint.
func detectCreateArchiveFormat(ref locator.Ref) archiveFormat {
	if archiveutil.HasZipHint(archiveutil.NameHint(ref)) {
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
	if archiveutil.HasZipHint(hint) {
		return archiveFormatZip
	}
	if isZipContentType(contentType) {
		return archiveFormatZip
	}
	return archiveFormatTar
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
