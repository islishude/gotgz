package compress

import (
	"bytes"
	"io"
	"testing"

	gzip "github.com/klauspost/pgzip"
)

func TestDetectByExtension(t *testing.T) {
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	if _, err := gw.Write([]byte("plain")); err != nil {
		t.Fatalf("gzip write error = %v", err)
	}
	if err := gw.Close(); err != nil {
		t.Fatalf("gzip close error = %v", err)
	}
	_, d, err := NewReader(io.NopCloser(bytes.NewReader(buf.Bytes())), Auto, "a.tar.gz", "")
	if err != nil {
		t.Fatalf("NewReader() error = %v", err)
	}
	if d != Gzip {
		t.Fatalf("detected = %q, want gzip", d)
	}
}

func TestDetectTypeByPath(t *testing.T) {
	tests := []struct {
		name string
		path string
		want Type
	}{
		{name: "tar gzip", path: "archive.tar.gz", want: Gzip},
		{name: "tgz alias", path: "archive.tgz", want: Gzip},
		{name: "gzip alias", path: "archive.gz", want: Gzip},
		{name: "tar bzip2", path: "archive.tar.bz2", want: Bzip2},
		{name: "tbz2 alias", path: "archive.tbz2", want: Bzip2},
		{name: "tbz alias", path: "archive.tbz", want: Bzip2},
		{name: "bz2 alias", path: "archive.bz2", want: Bzip2},
		{name: "tar xz", path: "archive.tar.xz", want: Xz},
		{name: "txz alias", path: "archive.txz", want: Xz},
		{name: "xz alias", path: "archive.xz", want: Xz},
		{name: "tar zst", path: "archive.tar.zst", want: Zstd},
		{name: "tzst alias", path: "archive.tzst", want: Zstd},
		{name: "zst alias", path: "archive.zst", want: Zstd},
		{name: "zstd alias", path: "archive.zstd", want: Zstd},
		{name: "tar lz4", path: "archive.tar.lz4", want: Lz4},
		{name: "tlz4 alias", path: "archive.tlz4", want: Lz4},
		{name: "lz4 alias", path: "archive.lz4", want: Lz4},
		{name: "tar none", path: "archive.tar", want: None},
		{name: "tape none", path: "archive.tape", want: None},
		{name: "upper case", path: "ARCHIVE.TAR.GZ", want: Gzip},
		{name: "unknown", path: "archive.bin", want: Auto},
		{name: "no extension", path: "archive", want: Auto},
		{name: "trim spaces", path: "  archive.tar.zst  ", want: Zstd},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := DetectTypeByPath(tt.path); got != tt.want {
				t.Fatalf("DetectTypeByPath(%q) = %q, want %q", tt.path, got, tt.want)
			}
		})
	}
}

func TestDetectByContentType(t *testing.T) {
	cases := []struct {
		contentType string
		want        Type
	}{
		{contentType: "application/gzip", want: Gzip},
		{contentType: "application/x-bzip2", want: Bzip2},
		{contentType: "application/x-xz", want: Xz},
		{contentType: "application/zstd", want: Zstd},
		{contentType: "application/x-lz4", want: Lz4},
		{contentType: "application/x-tar", want: None},
		{contentType: "text/plain", want: Auto},
	}
	for _, tc := range cases {
		t.Run(tc.contentType, func(t *testing.T) {
			got := detectByContentType(tc.contentType)
			if got != tc.want {
				t.Fatalf("detectByContentType(%q)=%q, want %q", tc.contentType, got, tc.want)
			}
		})
	}
}
