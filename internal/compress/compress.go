package compress

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"github.com/dsnet/compress/bzip2"
	"github.com/klauspost/compress/zstd"
	gzip "github.com/klauspost/pgzip"
	"github.com/ulikunitz/xz"
)

type Type string

const (
	Auto  Type = "auto"
	None  Type = "none"
	Gzip  Type = "gzip"
	Bzip2 Type = "bzip2"
	Xz    Type = "xz"
	Zstd  Type = "zstd"
)

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
	default:
		return Auto
	}
}

func NewWriter(dst io.WriteCloser, t Type) (io.WriteCloser, error) {
	switch t {
	case Auto, None:
		return dst, nil
	case Gzip:
		zw := gzip.NewWriter(dst)
		return &stackedWriteCloser{writer: zw, dst: dst, closeWriterFirst: true}, nil
	case Bzip2:
		zw, err := bzip2.NewWriter(dst, &bzip2.WriterConfig{Level: bzip2.BestSpeed})
		if err != nil {
			return nil, err
		}
		return &stackedWriteCloser{writer: zw, dst: dst, closeWriterFirst: true}, nil
	case Xz:
		zw, err := xz.NewWriter(dst)
		if err != nil {
			return nil, err
		}
		return &stackedWriteCloser{writer: zw, dst: dst, closeWriterFirst: true}, nil
	case Zstd:
		zw, err := zstd.NewWriter(dst)
		if err != nil {
			return nil, err
		}
		return &stackedWriteCloser{writer: zw, dst: dst, closeWriterFirst: true}, nil
	default:
		return nil, fmt.Errorf("unsupported compression type %q", t)
	}
}

func NewReader(src io.ReadCloser, explicit Type, hint string) (io.ReadCloser, Type, error) {
	if explicit != Auto {
		r, err := wrapReaderByType(src, explicit)
		return r, explicit, err
	}
	br := bufio.NewReader(src)
	magic, _ := br.Peek(8)
	t := detectByMagic(magic)
	if t == Auto {
		t = detectByExt(hint)
	}
	if t == Auto {
		t = None
	}
	wrapped, err := wrapReader(br, src, t)
	return wrapped, t, err
}

func wrapReaderByType(src io.ReadCloser, t Type) (io.ReadCloser, error) {
	if t == None {
		return src, nil
	}
	br := bufio.NewReader(src)
	return wrapReader(br, src, t)
}

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
		zr, err := xz.NewReader(reader)
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
	default:
		return nil, fmt.Errorf("unsupported compression type %q", t)
	}
}

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
	default:
		return Auto
	}
}

func detectByExt(name string) Type {
	ext := strings.ToLower(filepath.Ext(name))
	switch ext {
	case ".gz", ".tgz":
		return Gzip
	case ".bz2", ".tbz2", ".tbz":
		return Bzip2
	case ".xz", ".txz":
		return Xz
	case ".zst", ".tzst", ".zstd":
		return Zstd
	default:
		return Auto
	}
}

type readCloser struct {
	reader io.Reader
	closer io.Closer
}

func (r *readCloser) Read(p []byte) (int, error) { return r.reader.Read(p) }
func (r *readCloser) Close() error               { return r.closer.Close() }

type multiReadCloser struct {
	reader  io.Reader
	closers []io.Closer
}

func (m *multiReadCloser) Read(p []byte) (int, error) { return m.reader.Read(p) }

func (m *multiReadCloser) Close() error {
	var first error
	for _, c := range m.closers {
		if err := c.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

type stackedWriteCloser struct {
	writer           io.WriteCloser
	dst              io.Closer
	closeWriterFirst bool
}

func (w *stackedWriteCloser) Write(p []byte) (int, error) { return w.writer.Write(p) }

func (w *stackedWriteCloser) Close() error {
	var first error
	if w.closeWriterFirst {
		if err := w.writer.Close(); err != nil {
			first = err
		}
		if err := w.dst.Close(); err != nil && first == nil {
			first = err
		}
		return first
	}
	if err := w.dst.Close(); err != nil {
		first = err
	}
	if err := w.writer.Close(); err != nil && first == nil {
		first = err
	}
	return first
}
