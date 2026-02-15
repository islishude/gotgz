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
	"github.com/pierrec/lz4/v4"
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
	Lz4   Type = "lz4"
)

type WriterOptions struct {
	Level *int
}

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

func NewWriter(dst io.WriteCloser, t Type, opts WriterOptions) (io.WriteCloser, error) {
	level, hasLevel, err := normalizeLevel(opts.Level)
	if err != nil {
		return nil, err
	}
	switch t {
	case Auto, None:
		return dst, nil
	case Gzip:
		var zw *gzip.Writer
		if hasLevel {
			zw, err = gzip.NewWriterLevel(dst, level)
			if err != nil {
				return nil, err
			}
		} else {
			zw = gzip.NewWriter(dst)
		}
		return &stackedWriteCloser{writer: zw, dst: dst, closeWriterFirst: true}, nil
	case Bzip2:
		cfg := &bzip2.WriterConfig{Level: bzip2.DefaultCompression}
		if hasLevel {
			cfg.Level = level
		}
		zw, err := bzip2.NewWriter(dst, cfg)
		if err != nil {
			return nil, err
		}
		return &stackedWriteCloser{writer: zw, dst: dst, closeWriterFirst: true}, nil
	case Xz:
		var zw *xz.Writer
		if hasLevel {
			cfg := xz.WriterConfig{DictCap: xzDictCapForLevel(level)}
			zw, err = cfg.NewWriter(dst)
		} else {
			zw, err = xz.NewWriter(dst)
		}
		if err != nil {
			return nil, err
		}
		return &stackedWriteCloser{writer: zw, dst: dst, closeWriterFirst: true}, nil
	case Zstd:
		if hasLevel {
			zw, err := zstd.NewWriter(dst, zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(level)))
			if err != nil {
				return nil, err
			}
			return &stackedWriteCloser{writer: zw, dst: dst, closeWriterFirst: true}, nil
		}
		zw, err := zstd.NewWriter(dst)
		if err != nil {
			return nil, err
		}
		return &stackedWriteCloser{writer: zw, dst: dst, closeWriterFirst: true}, nil
	case Lz4:
		zw := lz4.NewWriter(dst)
		if hasLevel {
			if err := zw.Apply(lz4.CompressionLevelOption(lz4Level(level))); err != nil {
				return nil, err
			}
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
	case Lz4:
		zr := lz4.NewReader(reader)
		return &readCloser{reader: zr, closer: src}, nil
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
	case len(magic) >= 4 && bytes.Equal(magic[:4], []byte{0x04, 0x22, 0x4d, 0x18}):
		return Lz4
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
	case ".lz4", ".tlz4":
		return Lz4
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

func normalizeLevel(level *int) (int, bool, error) {
	if level == nil {
		return 0, false, nil
	}
	if *level < 1 || *level > 9 {
		return 0, false, fmt.Errorf("compression level must be between 1 and 9")
	}
	return *level, true, nil
}

func xzDictCapForLevel(level int) int {
	// Roughly aligned with common xz presets 1..9.
	switch level {
	case 1:
		return 256 << 10
	case 2:
		return 1 << 20
	case 3:
		return 2 << 20
	case 4:
		return 4 << 20
	case 5:
		return 4 << 20
	case 6:
		return 8 << 20
	case 7:
		return 8 << 20
	case 8:
		return 16 << 20
	default:
		return 32 << 20
	}
}

func lz4Level(level int) lz4.CompressionLevel {
	switch level {
	case 1:
		return lz4.Level1
	case 2:
		return lz4.Level2
	case 3:
		return lz4.Level3
	case 4:
		return lz4.Level4
	case 5:
		return lz4.Level5
	case 6:
		return lz4.Level6
	case 7:
		return lz4.Level7
	case 8:
		return lz4.Level8
	default:
		return lz4.Level9
	}
}
