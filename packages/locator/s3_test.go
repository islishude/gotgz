package locator

import (
	"net/url"
	"reflect"
	"testing"
)

func TestParseArchiveS3URI(t *testing.T) {
	ref, err := ParseArchive("s3://bucket/path/to/a.tar")
	if err != nil {
		t.Fatalf("ParseArchive() error = %v", err)
	}
	if ref.Kind != KindS3 || ref.Bucket != "bucket" || ref.Key != "path/to/a.tar" {
		t.Fatalf("unexpected ref: %+v", ref)
	}
}

func TestParseArchiveS3URIWithMetadataQuery(t *testing.T) {
	ref, err := ParseArchive("s3://bucket/to/path.tgz?key=value&team=platform")
	if err != nil {
		t.Fatalf("ParseArchive() error = %v", err)
	}
	if ref.Kind != KindS3 || ref.Bucket != "bucket" || ref.Key != "to/path.tgz" {
		t.Fatalf("unexpected ref: %+v", ref)
	}
	if len(ref.Metadata) != 2 {
		t.Fatalf("unexpected metadata length: %d", len(ref.Metadata))
	}
	if ref.Metadata["key"] != "value" || ref.Metadata["team"] != "platform" {
		t.Fatalf("unexpected metadata: %#v", ref.Metadata)
	}
}

func TestParseArchiveObjectARN(t *testing.T) {
	ref, err := ParseArchive("arn:aws:s3:::my-bucket/path/to/archive.tar")
	if err != nil {
		t.Fatalf("ParseArchive() error = %v", err)
	}
	if ref.Kind != KindS3 || ref.Bucket != "my-bucket" || ref.Key != "path/to/archive.tar" {
		t.Fatalf("unexpected ref: %+v", ref)
	}
}

func TestParseArchiveAccessPointARN(t *testing.T) {
	v := "arn:aws:s3:us-west-2:123456789012:accesspoint/myap/object/path/to/archive.tar"
	ref, err := ParseArchive(v)
	if err != nil {
		t.Fatalf("ParseArchive() error = %v", err)
	}
	if ref.Kind != KindS3 || ref.Key != "path/to/archive.tar" {
		t.Fatalf("unexpected ref: %+v", ref)
	}
}

func TestParseArchiveBadARN(t *testing.T) {
	_, err := ParseArchive("arn:aws:ec2:us-west-2:123456789012:instance/i-123")
	if err == nil {
		t.Fatalf("expected error")
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
