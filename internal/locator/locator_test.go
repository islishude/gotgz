package locator

import "testing"

func TestParseArchiveS3URI(t *testing.T) {
	ref, err := ParseArchive("s3://bucket/path/to/a.tar")
	if err != nil {
		t.Fatalf("ParseArchive() error = %v", err)
	}
	if ref.Kind != KindS3 || ref.Bucket != "bucket" || ref.Key != "path/to/a.tar" {
		t.Fatalf("unexpected ref: %+v", ref)
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
