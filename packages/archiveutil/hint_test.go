package archiveutil

import (
	"testing"

	"github.com/islishude/gotgz/packages/locator"
)

// TestNameHint returns the expected path-like hint for each locator kind.
func TestNameHint(t *testing.T) {
	tests := []struct {
		name string
		ref  locator.Ref
		want string
	}{
		{
			name: "local path",
			ref:  locator.Ref{Kind: locator.KindLocal, Path: "/tmp/archive.tar.gz"},
			want: "/tmp/archive.tar.gz",
		},
		{
			name: "s3 key",
			ref:  locator.Ref{Kind: locator.KindS3, Key: "backups/archive.tar.gz"},
			want: "backups/archive.tar.gz",
		},
		{
			name: "http url path only",
			ref:  locator.Ref{Kind: locator.KindHTTP, URL: "https://example.com/path/to/archive.zip?token=abc"},
			want: "/path/to/archive.zip",
		},
		{
			name: "http invalid url falls back",
			ref:  locator.Ref{Kind: locator.KindHTTP, URL: "https://exa mple.com/archive.zip"},
			want: "https://exa mple.com/archive.zip",
		},
		{
			name: "stdio/raw fallback",
			ref:  locator.Ref{Kind: locator.KindStdio, Raw: "-"},
			want: "-",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NameHint(tt.ref); got != tt.want {
				t.Fatalf("NameHint(%+v) = %q, want %q", tt.ref, got, tt.want)
			}
		})
	}
}

// TestHasZipHint reports whether the input clearly implies zip format.
func TestHasZipHint(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want bool
	}{
		{
			name: "plain zip path",
			in:   "archive.zip",
			want: true,
		},
		{
			name: "upper case zip path",
			in:   "ARCHIVE.ZIP",
			want: true,
		},
		{
			name: "url zip path with query",
			in:   "https://example.com/download/archive.zip?sig=123",
			want: true,
		},
		{
			name: "trim spaces",
			in:   "  archive.zip  ",
			want: true,
		},
		{
			name: "non zip path",
			in:   "archive.tar.gz",
			want: false,
		},
		{
			name: "empty",
			in:   "",
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := HasZipHint(tt.in); got != tt.want {
				t.Fatalf("HasZipHint(%q) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}
