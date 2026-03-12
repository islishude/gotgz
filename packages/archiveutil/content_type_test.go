package archiveutil

import "testing"

// TestContentTypeForKey verifies suffix-based content type detection.
func TestContentTypeForKey(t *testing.T) {
	cases := []struct {
		key  string
		want string
	}{
		{key: "archives/out.tar.gz", want: "application/gzip"},
		{key: "archives/out.tgz", want: "application/gzip"},
		{key: "archives/out.gz", want: "application/gzip"},
		{key: "archives/out.tar.bz2", want: "application/x-bzip2"},
		{key: "archives/out.tar.xz", want: "application/x-xz"},
		{key: "archives/out.tar.zst", want: "application/zstd"},
		{key: "archives/out.tar.lz4", want: "application/x-lz4"},
		{key: "archives/out.tar", want: "application/x-tar"},
		{key: "archives/out.tape", want: "application/x-tar"},
		{key: "archives/out.zip", want: "application/zip"},
		{key: "notes/readme.txt", want: "text/plain; charset=utf-8"},
		{key: "noext", want: "application/octet-stream"},
	}

	for _, tc := range cases {
		t.Run(tc.key, func(t *testing.T) {
			got := ContentTypeForKey(tc.key)
			if got != tc.want {
				t.Fatalf("ContentTypeForKey(%q)=%q, want %q", tc.key, got, tc.want)
			}
		})
	}
}
