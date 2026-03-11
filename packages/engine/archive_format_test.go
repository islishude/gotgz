package engine

import (
	"testing"

	"github.com/islishude/gotgz/packages/archiveutil"
	"github.com/islishude/gotgz/packages/locator"
)

func TestDetectCreateArchiveFormat(t *testing.T) {
	cases := []struct {
		name string
		ref  locator.Ref
		want archiveFormat
	}{
		{
			name: "Local zip",
			ref:  locator.Ref{Kind: locator.KindLocal, Path: "/tmp/out.zip"},
			want: archiveFormatZip,
		},
		{
			name: "S3 zip",
			ref:  locator.Ref{Kind: locator.KindS3, Key: "backups/out.zip"},
			want: archiveFormatZip,
		},
		{
			name: "Tar fallback",
			ref:  locator.Ref{Kind: locator.KindLocal, Path: "/tmp/out.tar"},
			want: archiveFormatTar,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := detectCreateArchiveFormat(tc.ref); got != tc.want {
				t.Fatalf("detectCreateArchiveFormat() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestDetectReadArchiveFormat(t *testing.T) {
	cases := []struct {
		name        string
		magic       []byte
		hint        string
		contentType string
		want        archiveFormat
	}{
		{
			name:  "ZIP magic local-file-header",
			magic: []byte{'P', 'K', 0x03, 0x04},
			hint:  "a.tar",
			want:  archiveFormatZip,
		},
		{
			name:  "ZIP magic end-central-directory",
			magic: []byte{'P', 'K', 0x05, 0x06},
			hint:  "a.tar",
			want:  archiveFormatZip,
		},
		{
			name:  "Hint extension zip",
			magic: []byte{0x1f, 0x8b},
			hint:  "https://example.com/a.zip?sig=x",
			want:  archiveFormatZip,
		},
		{
			name:        "Content type zip",
			magic:       []byte("not-zip"),
			hint:        "stream.bin",
			contentType: "application/zip; charset=binary",
			want:        archiveFormatZip,
		},
		{
			name:  "Fallback tar",
			magic: []byte("not-zip"),
			hint:  "archive.tar",
			want:  archiveFormatTar,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := detectReadArchiveFormat(tc.magic, tc.hint, tc.contentType); got != tc.want {
				t.Fatalf("detectReadArchiveFormat() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestArchiveNameHint(t *testing.T) {
	httpRef := locator.Ref{
		Kind: locator.KindHTTP,
		URL:  "https://example.com/path/to/archive.zip?token=abc",
	}
	if got := archiveutil.NameHint(httpRef); got != "/path/to/archive.zip" {
		t.Fatalf("archiveNameHint(http) = %q", got)
	}

	s3Ref := locator.Ref{
		Kind: locator.KindS3,
		Key:  "path/to/archive.zip",
	}
	if got := archiveutil.NameHint(s3Ref); got != "path/to/archive.zip" {
		t.Fatalf("archiveNameHint(s3) = %q", got)
	}
}
