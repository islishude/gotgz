package compress

import (
	"bufio"
	"fmt"
	"io"

	"github.com/dsnet/compress/bzip2"
	"github.com/klauspost/compress/zstd"
	gzip "github.com/klauspost/pgzip"
	xzreader "github.com/mikelolasagasti/xz"
	"github.com/pierrec/lz4/v4"
)

// NewReader wraps src with decompression according to explicit mode or auto detection.
// Auto detection uses this order: magic bytes, filename hint extension, then content type.
func NewReader(src io.ReadCloser, explicit Type, hint string, contentType string) (io.ReadCloser, Type, error) {
	br := bufio.NewReader(src)
	magic, err := br.Peek(8)
	if err != nil && err != io.EOF {
		return nil, Auto, err
	}
	detected := detectByMagic(magic)

	if explicit != Auto {
		if detected != Auto && detected != explicit {
			return nil, explicit, fmt.Errorf("compression %q does not match archive data (detected %q)", explicit, detected)
		}
		wrapped, err := wrapReader(br, src, explicit)
		return wrapped, explicit, err
	}

	t := detected
	if t == Auto {
		t = DetectTypeByPath(hint)
	}
	if t == Auto {
		t = detectByContentType(contentType)
	}
	if t == Auto {
		t = None
	}
	wrapped, err := wrapReader(br, src, t)
	return wrapped, t, err
}

// wrapReader creates a decompression reader for the given type, layered on top of src.
func wrapReader(reader io.Reader, src io.Closer, t Type) (io.ReadCloser, error) {
	switch t {
	case None:
		return &readCloser{reader: reader, closer: src}, nil
	case Gzip:
		zr, err := gzip.NewReader(reader)
		if err != nil {
			return nil, err
		}
		return &multiReadCloser{reader: zr, closers: []io.Closer{zr, src}}, nil
	case Bzip2:
		zr, err := bzip2.NewReader(reader, nil)
		if err != nil {
			return nil, err
		}
		return &readCloser{reader: zr, closer: src}, nil
	case Xz:
		// XZ decode intentionally uses mikelolasagasti/xz while encode stays on ulikunitz/xz.
		zr, err := xzreader.NewReader(reader, 0)
		if err != nil {
			return nil, err
		}
		return &readCloser{reader: zr, closer: src}, nil
	case Zstd:
		zr, err := zstd.NewReader(reader)
		if err != nil {
			return nil, err
		}
		return &multiReadCloser{reader: zr, closers: []io.Closer{zr.IOReadCloser(), src}}, nil
	case Lz4:
		zr := lz4.NewReader(reader)
		return &readCloser{reader: zr, closer: src}, nil
	default:
		return nil, fmt.Errorf("unsupported compression type %q", t)
	}
}

// readCloser pairs a reader with a separate closer (typically the original source).
type readCloser struct {
	reader io.Reader
	closer io.Closer
}

// Read delegates to the underlying reader.
func (r *readCloser) Read(p []byte) (int, error) { return r.reader.Read(p) }

// Close closes the underlying source.
func (r *readCloser) Close() error { return r.closer.Close() }

// multiReadCloser pairs a reader with multiple closers that are all closed in order.
type multiReadCloser struct {
	reader  io.Reader
	closers []io.Closer
}

// Read delegates to the underlying reader.
func (m *multiReadCloser) Read(p []byte) (int, error) { return m.reader.Read(p) }

// Close closes all closers in order and returns the first error encountered.
func (m *multiReadCloser) Close() error {
	var first error
	for _, c := range m.closers {
		if err := c.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}
