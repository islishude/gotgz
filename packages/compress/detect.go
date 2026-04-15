package compress

import (
	"bytes"
	"strings"
)

// detectByMagic identifies the compression type from the first bytes of a stream.
func detectByMagic(magic []byte) Type {
	switch {
	case len(magic) >= 2 && bytes.Equal(magic[:2], []byte{0x1f, 0x8b}):
		return Gzip
	case len(magic) >= 3 && bytes.Equal(magic[:3], []byte{'B', 'Z', 'h'}):
		return Bzip2
	case len(magic) >= 6 && bytes.Equal(magic[:6], []byte{0xfd, '7', 'z', 'X', 'Z', 0x00}):
		return Xz
	case len(magic) >= 4 && bytes.Equal(magic[:4], []byte{0x28, 0xb5, 0x2f, 0xfd}):
		return Zstd
	case len(magic) >= 4 && bytes.Equal(magic[:4], []byte{0x04, 0x22, 0x4d, 0x18}):
		return Lz4
	default:
		return Auto
	}
}

// DetectTypeByPath returns the compression type implied by a path-like hint.
//
// It recognizes the same tar-family suffix aliases for both create-time
// validation and read-time auto-detection. `.tar` and `.tape` explicitly mean
// an uncompressed tar stream, while unknown suffixes return Auto.
func DetectTypeByPath(name string) Type {
	hint := strings.ToLower(strings.TrimSpace(name))
	switch {
	case strings.HasSuffix(hint, ".tar.gz"), strings.HasSuffix(hint, ".tgz"), strings.HasSuffix(hint, ".gz"):
		return Gzip
	case strings.HasSuffix(hint, ".tar.bz2"), strings.HasSuffix(hint, ".tbz2"), strings.HasSuffix(hint, ".tbz"), strings.HasSuffix(hint, ".bz2"):
		return Bzip2
	case strings.HasSuffix(hint, ".tar.xz"), strings.HasSuffix(hint, ".txz"), strings.HasSuffix(hint, ".xz"):
		return Xz
	case strings.HasSuffix(hint, ".tar.zst"), strings.HasSuffix(hint, ".tzst"), strings.HasSuffix(hint, ".zstd"), strings.HasSuffix(hint, ".zst"):
		return Zstd
	case strings.HasSuffix(hint, ".tar.lz4"), strings.HasSuffix(hint, ".tlz4"), strings.HasSuffix(hint, ".lz4"):
		return Lz4
	case strings.HasSuffix(hint, ".tar"), strings.HasSuffix(hint, ".tape"):
		return None
	default:
		return Auto
	}
}

// detectByContentType maps common archive media types to compression types.
func detectByContentType(v string) Type {
	ct := strings.ToLower(strings.TrimSpace(v))
	if ct == "" {
		return Auto
	}
	mediaType, _, _ := strings.Cut(ct, ";")
	mediaType = strings.TrimSpace(mediaType)
	switch mediaType {
	case "application/gzip", "application/x-gzip":
		return Gzip
	case "application/x-bzip2":
		return Bzip2
	case "application/x-xz":
		return Xz
	case "application/zstd", "application/x-zstd":
		return Zstd
	case "application/x-lz4":
		return Lz4
	case "application/x-tar":
		return None
	default:
		return Auto
	}
}
