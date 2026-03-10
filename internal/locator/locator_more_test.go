package locator

import (
	"net/url"
	"reflect"
	"testing"
)

// TestParseArchiveLocalAndStdio verifies that plain paths stay local and "-"
// is treated as the stdio archive sentinel.
func TestParseArchiveLocalAndStdio(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  Ref
	}{
		{
			name:  "stdio",
			input: "-",
			want:  Ref{Kind: KindStdio, Raw: "-"},
		},
		{
			name:  "local path",
			input: "archives/out.tar",
			want:  Ref{Kind: KindLocal, Raw: "archives/out.tar", Path: "archives/out.tar"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseArchive(tt.input)
			if err != nil {
				t.Fatalf("ParseArchive() error = %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("ParseArchive() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

// TestParseArchiveRejectsMissingS3Bucket verifies that malformed S3 URIs fail
// instead of producing an incomplete reference.
func TestParseArchiveRejectsMissingS3Bucket(t *testing.T) {
	if _, err := ParseArchive("s3:///archive.tar"); err == nil {
		t.Fatalf("ParseArchive() error = nil, want non-nil")
	}
}

// TestParseArchiveObjectARNBucketNamedBucket verifies that object ARNs are
// parsed correctly even when the bucket name itself is "bucket".
func TestParseArchiveObjectARNBucketNamedBucket(t *testing.T) {
	ref, err := ParseArchive("arn:aws:s3:::bucket/path/to/file.txt")
	if err != nil {
		t.Fatalf("ParseArchive() error = %v", err)
	}
	if ref.Kind != KindS3 || ref.Bucket != "bucket" || ref.Key != "path/to/file.txt" {
		t.Fatalf("ParseArchive() = %+v, want bucket=%q key=%q", ref, "bucket", "path/to/file.txt")
	}
}

// TestParseMemberParsesSupportedRefs verifies that member inputs accept local
// paths and S3 object references.
func TestParseMemberParsesSupportedRefs(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  Ref
	}{
		{
			name:  "local path",
			input: "content/file.txt",
			want:  Ref{Kind: KindLocal, Raw: "content/file.txt", Path: "content/file.txt"},
		},
		{
			name:  "s3 uri",
			input: "s3://bucket/path/to/file.txt",
			want:  Ref{Kind: KindS3, Raw: "s3://bucket/path/to/file.txt", Bucket: "bucket", Key: "path/to/file.txt"},
		},
		{
			name:  "object arn",
			input: "arn:aws:s3:::my-bucket/path/to/file.txt",
			want:  Ref{Kind: KindS3, Raw: "arn:aws:s3:::my-bucket/path/to/file.txt", Bucket: "my-bucket", Key: "path/to/file.txt"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseMember(tt.input)
			if err != nil {
				t.Fatalf("ParseMember() error = %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("ParseMember() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

// TestJoinS3Prefix verifies that prefixes and member names are normalized into
// a single S3 object key.
func TestJoinS3Prefix(t *testing.T) {
	tests := []struct {
		name   string
		prefix string
		member string
		want   string
	}{
		{name: "empty prefix", prefix: "", member: "file.txt", want: "file.txt"},
		{name: "empty member", prefix: "prefix", member: "", want: "prefix"},
		{name: "trim slashes", prefix: "/prefix/", member: "/file.txt", want: "prefix/file.txt"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := JoinS3Prefix(tt.prefix, tt.member); got != tt.want {
				t.Fatalf("JoinS3Prefix(%q, %q) = %q, want %q", tt.prefix, tt.member, got, tt.want)
			}
		})
	}
}

// TestParseArchiveRejectsAccessPointARNWithoutObjectKey verifies that access
// point ARNs without an object path are rejected.
func TestParseArchiveRejectsAccessPointARNWithoutObjectKey(t *testing.T) {
	_, err := ParseArchive("arn:aws:s3:us-west-2:123456789012:accesspoint/myap/object/")
	if err == nil {
		t.Fatalf("ParseArchive() error = nil, want non-nil")
	}
}

// TestParseQueryMetadata verifies that blank keys are ignored, keys are
// trimmed, and repeated query values are joined predictably.
func TestParseQueryMetadata(t *testing.T) {
	t.Run("empty query returns nil", func(t *testing.T) {
		if got := parseQueryMetadata(nil); got != nil {
			t.Fatalf("parseQueryMetadata(nil) = %#v, want nil", got)
		}
	})

	t.Run("metadata is normalized", func(t *testing.T) {
		q := url.Values{
			"":       {"skip"},
			" team ": {"platform", "ops"},
			"flag":   {},
		}
		want := map[string]string{
			"flag": "",
			"team": "platform,ops",
		}

		if got := parseQueryMetadata(q); !reflect.DeepEqual(got, want) {
			t.Fatalf("parseQueryMetadata() = %#v, want %#v", got, want)
		}
	})
}
