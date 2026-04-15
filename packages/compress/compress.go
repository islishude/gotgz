package compress

import (
	"io"
	"strings"
)

// Type represents a compression algorithm identifier.
type Type string

const (
	Auto  Type = "auto"
	None  Type = "none"
	Gzip  Type = "gzip"
	Bzip2 Type = "bzip2"
	Xz    Type = "xz"
	Zstd  Type = "zstd"
	Lz4   Type = "lz4"
)

// WriterOptions holds optional configuration for compression writers.
type WriterOptions struct {
	Level *int
}

// FlushWriteCloser exposes stream flush support when the compression format has it.
type FlushWriteCloser interface {
	io.WriteCloser
	Flush() error
}

// FromString maps a human-readable name to a compression Type constant.
// Unknown values fall back to Auto.
func FromString(v string) Type {
	switch strings.ToLower(v) {
	case "none":
		return None
	case "gzip":
		return Gzip
	case "bzip2":
		return Bzip2
	case "xz":
		return Xz
	case "zstd":
		return Zstd
	case "lz4":
		return Lz4
	default:
		return Auto
	}
}
