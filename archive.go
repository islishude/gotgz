package gotgz

import (
	"compress/gzip"
	"fmt"
	"io"
	"net/url"
	"strconv"

	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

type Archiver interface {
	Name() string
	MediaType() string
	Writer(io.WriteCloser) (io.WriteCloser, error)
	Reader(io.ReadCloser) (io.Reader, error)
	Extension() string
}

type Optioner interface {
	Get(string) string
}

type GZipArchiver struct {
	Level int
}

func NewGZip(query Optioner) (GZipArchiver, error) {
	level := gzip.DefaultCompression
	if levelQuery := query.Get("level"); levelQuery != "" {
		var err error
		level, err = strconv.Atoi(levelQuery)
		if err != nil {
			return GZipArchiver{}, err
		}
	}
	return GZipArchiver{Level: level}, nil
}

func (GZipArchiver) MediaType() string {
	return "application/gzip"
}

func (g GZipArchiver) Writer(w io.WriteCloser) (io.WriteCloser, error) {
	return gzip.NewWriterLevel(w, g.Level)
}

func (GZipArchiver) Reader(r io.ReadCloser) (io.Reader, error) {
	return gzip.NewReader(r)
}

func (GZipArchiver) Extension() string {
	return ".gz"
}

func (GZipArchiver) Name() string {
	return "gzip"
}

func (g *GZipArchiver) ParseOptions(query url.Values) (err error) {
	var level = gzip.DefaultCompression
	if levelQuery := query.Get("level"); levelQuery != "" {
		level, err = strconv.Atoi(levelQuery)
		if err != nil {
			return err
		}
	}
	g.Level = level
	return nil
}

type Lz4Archiver struct {
	Level int
}

func NewLz4(query Optioner) (Lz4Archiver, error) {
	var res = Lz4Archiver{}
	if levelQuery := query.Get("level"); levelQuery != "" {
		var err error
		l, err := strconv.Atoi(levelQuery)
		if err != nil {
			return Lz4Archiver{}, err
		}
		if l < 0 || l > 9 {
			return Lz4Archiver{}, fmt.Errorf("invalid lz4 compression level: %d", l)
		}
		res.Level = int(l)
	}
	return res, nil
}

func (Lz4Archiver) MediaType() string {
	return "application/lz4"
}

func (l Lz4Archiver) Writer(w io.WriteCloser) (io.WriteCloser, error) {
	lzw := lz4.NewWriter(w)
	var opts []lz4.Option
	if l.Level > 0 && l.Level < 9 {
		opts = append(opts, lz4.CompressionLevelOption(1<<(8+l.Level)))
	}
	if err := lzw.Apply(opts...); err != nil {
		return nil, err
	}
	return lzw, nil
}

func (l Lz4Archiver) Reader(r io.ReadCloser) (io.Reader, error) {
	return lz4.NewReader(r), nil
}

func (Lz4Archiver) Extension() string {
	return ".lz4"
}

func (Lz4Archiver) Name() string {
	return "lz4"
}

type ZstdArchiver struct {
	Level int
}

func NewZstd(query Optioner) (ZstdArchiver, error) {
	var res = ZstdArchiver{}
	if levelQuery := query.Get("level"); levelQuery != "" {
		var err error
		l, err := strconv.Atoi(levelQuery)
		if err != nil {
			return res, err
		}
		res.Level = l
	}
	return res, nil
}

func (ZstdArchiver) MediaType() string {
	return "application/zstd"
}

func (z ZstdArchiver) Writer(w io.WriteCloser) (io.WriteCloser, error) {
	zd, err := zstd.NewWriter(w, zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(z.Level)))
	return zd, err
}

func (z ZstdArchiver) Reader(r io.ReadCloser) (io.Reader, error) {
	zr, err := zstd.NewReader(r)
	if err != nil {
		return nil, err
	}
	return zr, nil
}

func (ZstdArchiver) Extension() string {
	return ".zst"
}

func (ZstdArchiver) Name() string {
	return "zstd"
}
