package compress

import (
	"fmt"
	"io"

	"github.com/dsnet/compress/bzip2"
	"github.com/klauspost/compress/zstd"
	gzip "github.com/klauspost/pgzip"
	"github.com/pierrec/lz4/v4"
	xzwriter "github.com/ulikunitz/xz"
)

// NewWriter returns a compression writer that wraps dst using the given type.
// When t is Auto or None the data is passed through uncompressed.
func NewWriter(dst io.WriteCloser, t Type, opts WriterOptions) (io.WriteCloser, error) {
	level, hasLevel, err := normalizeLevel(opts.Level)
	if err != nil {
		return nil, err
	}
	switch t {
	case Auto, None:
		return &plainWriteCloser{dst: dst}, nil
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
		var zw *xzwriter.Writer
		if hasLevel {
			cfg := xzwriter.WriterConfig{DictCap: xzDictCapForLevel(level)}
			zw, err = cfg.NewWriter(dst)
		} else {
			zw, err = xzwriter.NewWriter(dst)
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

// stackedWriteCloser pairs a compression writer with its underlying destination
// and closes them in the configured order.
type stackedWriteCloser struct {
	writer           io.WriteCloser
	dst              io.Closer
	closeWriterFirst bool
}

// Write delegates to the compression writer.
func (w *stackedWriteCloser) Write(p []byte) (int, error) { return w.writer.Write(p) }

// Flush flushes the compression writer when it supports FlushWriteCloser.
func (w *stackedWriteCloser) Flush() error {
	if flusher, ok := w.writer.(FlushWriteCloser); ok {
		return flusher.Flush()
	}
	return nil
}

// Close closes the writer and destination in the configured order, returning the first error.
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

// plainWriteCloser passes data through without compression.
type plainWriteCloser struct {
	dst io.WriteCloser
}

// Write delegates to the destination writer.
func (w *plainWriteCloser) Write(p []byte) (int, error) { return w.dst.Write(p) }

// Flush is a no-op for uncompressed streams.
func (w *plainWriteCloser) Flush() error { return nil }

// Close closes the underlying destination writer.
func (w *plainWriteCloser) Close() error { return w.dst.Close() }

// normalizeLevel validates and returns the compression level from an optional pointer.
func normalizeLevel(level *int) (int, bool, error) {
	if level == nil {
		return 0, false, nil
	}
	if *level < 1 || *level > 9 {
		return 0, false, fmt.Errorf("compression level must be between 1 and 9")
	}
	return *level, true, nil
}

// xzDictCapForLevel maps compression levels 1-9 to xz dictionary capacities roughly
// aligned with common xz presets.
func xzDictCapForLevel(level int) int {
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

// lz4Level maps a generic 1-9 compression level to a lz4.CompressionLevel constant.
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
