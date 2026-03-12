package archiveutil

import (
	"strings"

	"github.com/islishude/gotgz/packages/locator"
)

type ArchiveFormat string

const (
	ArchiveFormatTar ArchiveFormat = "tar"
	ArchiveFormatZip ArchiveFormat = "zip"
)

// DetectCreateArchiveFormat resolves output archive format from destination hint.
func DetectCreateArchiveFormat(ref locator.Ref) ArchiveFormat {
	if HasZipHint(NameHint(ref)) {
		return ArchiveFormatZip
	}
	return ArchiveFormatTar
}

// DetectReadArchiveFormat resolves input archive format by magic, then hint,
// then content type.
func DetectReadArchiveFormat(magic []byte, hint string, contentType string) ArchiveFormat {
	if IsZipMagic(magic) {
		return ArchiveFormatZip
	}
	if HasZipHint(hint) {
		return ArchiveFormatZip
	}
	if IsZipContentType(contentType) {
		return ArchiveFormatZip
	}
	return ArchiveFormatTar
}

// IsZipMagic reports whether the provided bytes match ZIP local/central signatures.
func IsZipMagic(magic []byte) bool {
	if len(magic) < 4 {
		return false
	}
	return (magic[0] == 'P' && magic[1] == 'K' && magic[2] == 0x03 && magic[3] == 0x04) ||
		(magic[0] == 'P' && magic[1] == 'K' && magic[2] == 0x05 && magic[3] == 0x06) ||
		(magic[0] == 'P' && magic[1] == 'K' && magic[2] == 0x07 && magic[3] == 0x08)
}

// IsZipContentType reports whether HTTP/S3 content type implies zip format.
func IsZipContentType(v string) bool {
	ct := strings.ToLower(strings.TrimSpace(v))
	if ct == "" {
		return false
	}
	mediaType, _, _ := strings.Cut(ct, ";")
	mediaType = strings.TrimSpace(mediaType)
	return mediaType == "application/zip" || mediaType == "application/x-zip-compressed"
}
