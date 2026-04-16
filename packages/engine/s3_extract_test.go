package engine

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/islishude/gotgz/packages/locator"
)

func TestExtractToS3RegularFile(t *testing.T) {
	var uploadedRef locator.Ref
	var uploadedBody string
	var uploadedMeta map[string]string

	r := &Runner{
		storage: &storageRouter{
			s3: fakeS3ArchiveStore{
				uploadStream: func(_ context.Context, ref locator.Ref, body io.Reader, metadata map[string]string) error {
					uploadedRef = ref
					payload, err := io.ReadAll(body)
					if err != nil {
						return err
					}
					uploadedBody = string(payload)
					uploadedMeta = metadata
					return nil
				},
			},
		},
		stderr: io.Discard,
		stdout: io.Discard,
	}

	hdr := &tar.Header{Name: "dir/file.txt", Mode: 0o644, Size: 7, Typeflag: tar.TypeReg, Format: tar.FormatPAX}
	tr := newTarReaderFromEntries(t, []tarEntry{{hdr: hdr, body: "payload"}})
	if _, err := tr.Next(); err != nil {
		t.Fatalf("Next() error = %v", err)
	}

	warnings, err := r.extractToS3(context.Background(), locator.Ref{Kind: locator.KindS3, Bucket: "bucket", Key: "prefix"}, hdr, tr, nil)
	if err != nil {
		t.Fatalf("extractToS3() error = %v", err)
	}
	if warnings != 0 {
		t.Fatalf("warnings = %d, want 0", warnings)
	}
	if uploadedRef.Bucket != "bucket" || !strings.Contains(uploadedRef.Key, "dir/file.txt") {
		t.Fatalf("uploadedRef = %+v", uploadedRef)
	}
	if uploadedBody != "payload" {
		t.Fatalf("uploadedBody = %q, want payload", uploadedBody)
	}
	if uploadedMeta == nil {
		t.Fatal("uploadedMeta should not be nil")
	}
}

func TestExtractToS3DirectoryEntry(t *testing.T) {
	r := &Runner{
		storage: &storageRouter{
			s3: fakeS3ArchiveStore{
				uploadStream: func(_ context.Context, _ locator.Ref, _ io.Reader, _ map[string]string) error {
					t.Fatal("uploadStream should not be called for directories")
					return nil
				},
			},
		},
		stderr: io.Discard,
		stdout: io.Discard,
	}

	hdr := &tar.Header{Name: "dir/", Mode: 0o755, Typeflag: tar.TypeDir, Format: tar.FormatPAX}
	tr := newTarReaderFromEntries(t, []tarEntry{{hdr: hdr}})
	_, _ = tr.Next()

	warnings, err := r.extractToS3(context.Background(), locator.Ref{Kind: locator.KindS3, Bucket: "b", Key: "p"}, hdr, tr, nil)
	if err != nil {
		t.Fatalf("extractToS3() error = %v", err)
	}
	if warnings != 0 {
		t.Fatalf("warnings = %d, want 0", warnings)
	}
}

func TestExtractToS3EmptyNameWithContent(t *testing.T) {
	uploaded := false
	r := &Runner{
		storage: &storageRouter{
			s3: fakeS3ArchiveStore{
				uploadStream: func(_ context.Context, _ locator.Ref, _ io.Reader, _ map[string]string) error {
					uploaded = true
					return nil
				},
			},
		},
		stderr: io.Discard,
		stdout: io.Discard,
	}

	hdr := &tar.Header{Name: "./", Mode: 0o755, Size: 0, Typeflag: tar.TypeDir, Format: tar.FormatGNU}
	tr := tar.NewReader(&bytes.Buffer{})

	warnings, err := r.extractToS3(context.Background(), locator.Ref{Kind: locator.KindS3, Bucket: "b", Key: "p"}, hdr, tr, nil)
	if err != nil {
		t.Fatalf("extractToS3() error = %v", err)
	}
	if warnings != 0 {
		t.Fatalf("warnings = %d, want 0", warnings)
	}
	if uploaded {
		t.Fatal("uploadStream should not be called for empty names")
	}
}

func TestExtractToS3SymlinkEntry(t *testing.T) {
	var uploadedRef locator.Ref
	var uploadedMeta map[string]string

	r := &Runner{
		storage: &storageRouter{
			s3: fakeS3ArchiveStore{
				uploadStream: func(_ context.Context, ref locator.Ref, body io.Reader, metadata map[string]string) error {
					uploadedRef = ref
					uploadedMeta = metadata
					_, _ = io.ReadAll(body)
					return nil
				},
			},
		},
		stderr: io.Discard,
		stdout: io.Discard,
	}

	hdr := &tar.Header{Name: "link.txt", Mode: 0o777, Typeflag: tar.TypeSymlink, Linkname: "real.txt", Format: tar.FormatPAX}
	tr := newTarReaderFromEntries(t, []tarEntry{{hdr: hdr}})
	_, _ = tr.Next()

	warnings, err := r.extractToS3(context.Background(), locator.Ref{Kind: locator.KindS3, Bucket: "b", Key: "p"}, hdr, tr, nil)
	if err != nil {
		t.Fatalf("extractToS3() error = %v", err)
	}
	if warnings != 0 {
		t.Fatalf("warnings = %d, want 0", warnings)
	}
	if !strings.Contains(uploadedRef.Key, "link.txt") {
		t.Fatalf("uploadedRef = %+v", uploadedRef)
	}
	if uploadedMeta["gotgz-type"] != "50" {
		t.Fatalf("gotgz-type = %q, want 50", uploadedMeta["gotgz-type"])
	}
}

func TestExtractToS3UploadError(t *testing.T) {
	wantErr := errors.New("upload failed")
	r := &Runner{
		storage: &storageRouter{
			s3: fakeS3ArchiveStore{
				uploadStream: func(_ context.Context, _ locator.Ref, _ io.Reader, _ map[string]string) error {
					return wantErr
				},
			},
		},
		stderr: io.Discard,
		stdout: io.Discard,
	}

	hdr := newTarHeader("file.txt", 4, 0o644)
	tr := newTarReaderFromEntries(t, []tarEntry{{hdr: hdr, body: "test"}})
	_, _ = tr.Next()

	_, err := r.extractToS3(context.Background(), locator.Ref{Kind: locator.KindS3, Bucket: "b", Key: "p"}, hdr, tr, nil)
	if !errors.Is(err, wantErr) {
		t.Fatalf("err = %v, want %v", err, wantErr)
	}
}
