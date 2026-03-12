package locator

import (
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
