package engine

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/islishude/gotgz/packages/locator"
	httpstore "github.com/islishude/gotgz/packages/storage/http"
	localstore "github.com/islishude/gotgz/packages/storage/local"
	s3store "github.com/islishude/gotgz/packages/storage/s3"
)

func TestStorageRouterOpenArchiveReaderHTTPUnknownSize(t *testing.T) {
	router := &storageRouter{
		http: fakeHTTPArchiveStore{
			openReader: func(_ context.Context, ref locator.Ref) (io.ReadCloser, httpstore.Metadata, error) {
				if ref.URL != "https://example.test/archive.tar" {
					t.Fatalf("ref.URL = %q", ref.URL)
				}
				return io.NopCloser(strings.NewReader("http")), httpstore.Metadata{Size: -1, ContentType: "application/x-tar"}, nil
			},
		},
	}

	rc, info, err := router.openArchiveReader(context.Background(), locator.Ref{Kind: locator.KindHTTP, Raw: "https://example.test/archive.tar", URL: "https://example.test/archive.tar"})
	if err != nil {
		t.Fatalf("openArchiveReader() error = %v", err)
	}
	defer rc.Close() //nolint:errcheck
	if info.SizeKnown {
		t.Fatalf("SizeKnown = true, want false")
	}
	if info.ContentType != "application/x-tar" {
		t.Fatalf("ContentType = %q", info.ContentType)
	}
}

func TestStorageRouterOpenArchiveWriterRejectsHTTP(t *testing.T) {
	router := &storageRouter{}
	_, err := router.openArchiveWriter(context.Background(), locator.Ref{Kind: locator.KindHTTP, Raw: "https://example.test/archive.tar"})
	if err == nil || !strings.Contains(err.Error(), "source-only") {
		t.Fatalf("openArchiveWriter() err = %v, want source-only error", err)
	}
}

func TestNewStorageRouterWiresZipRangeStores(t *testing.T) {
	router := newStorageRouter(
		nil,
		fakeS3ZipArchiveStore{
			openRange: func(_ context.Context, _ locator.Ref, _, _ int64) (io.ReadCloser, error) {
				return io.NopCloser(strings.NewReader("s3-range")), nil
			},
		},
		fakeHTTPZipArchiveStore{
			openRange: func(_ context.Context, _ locator.Ref, _, _ int64) (io.ReadCloser, error) {
				return io.NopCloser(strings.NewReader("http-range")), nil
			},
		},
	)

	s3rc, err := router.openZipRangeReader(context.Background(), locator.Ref{
		Kind:   locator.KindS3,
		Raw:    "s3://bucket/archive.zip",
		Bucket: "bucket",
		Key:    "archive.zip",
	}, 4, 8)
	if err != nil {
		t.Fatalf("openZipRangeReader() s3 error = %v", err)
	}
	defer s3rc.Close() //nolint:errcheck
	s3body, err := io.ReadAll(s3rc)
	if err != nil {
		t.Fatalf("io.ReadAll(s3rc) error = %v", err)
	}
	if string(s3body) != "s3-range" {
		t.Fatalf("s3 body = %q", s3body)
	}

	httprc, err := router.openZipRangeReader(context.Background(), locator.Ref{
		Kind: locator.KindHTTP,
		Raw:  "https://example.test/archive.zip",
		URL:  "https://example.test/archive.zip",
	}, 2, 6)
	if err != nil {
		t.Fatalf("openZipRangeReader() http error = %v", err)
	}
	defer httprc.Close() //nolint:errcheck
	httpbody, err := io.ReadAll(httprc)
	if err != nil {
		t.Fatalf("io.ReadAll(httprc) error = %v", err)
	}
	if string(httpbody) != "http-range" {
		t.Fatalf("http body = %q", httpbody)
	}
}

func TestStorageRouterOpenZipRangeReaderRequiresConfiguredRangeStore(t *testing.T) {
	router := newStorageRouter(nil, fakeS3ArchiveStore{}, fakeHTTPArchiveStore{})

	_, err := router.openZipRangeReader(context.Background(), locator.Ref{
		Kind:   locator.KindS3,
		Raw:    "s3://bucket/archive.zip",
		Bucket: "bucket",
		Key:    "archive.zip",
	}, 0, 1)
	if err == nil || !strings.Contains(err.Error(), "zip range store is not configured") {
		t.Fatalf("openZipRangeReader() err = %v", err)
	}
}

func TestStorageRouterReturnsConfiguredBackendErrors(t *testing.T) {
	ctx := context.Background()

	t.Run("local missing", func(t *testing.T) {
		router := &storageRouter{}
		_, _, err := router.openArchiveReader(ctx, locator.Ref{Kind: locator.KindLocal, Raw: "archive.tar", Path: "archive.tar"})
		if err == nil || !strings.Contains(err.Error(), "local archive store is not configured") {
			t.Fatalf("openArchiveReader() err = %v", err)
		}
	})

	t.Run("s3 missing", func(t *testing.T) {
		router := &storageRouter{}
		_, err := router.openArchiveWriter(ctx, locator.Ref{Kind: locator.KindS3, Raw: "s3://bucket/key", Bucket: "bucket", Key: "key"})
		if err == nil || !strings.Contains(err.Error(), "s3 archive store is not configured") {
			t.Fatalf("openArchiveWriter() err = %v", err)
		}
	})

	t.Run("http missing", func(t *testing.T) {
		router := &storageRouter{}
		_, _, err := router.openArchiveReader(ctx, locator.Ref{Kind: locator.KindHTTP, Raw: "https://example.test/archive.tar", URL: "https://example.test/archive.tar"})
		if err == nil || !strings.Contains(err.Error(), "http archive store is not configured") {
			t.Fatalf("openArchiveReader() err = %v", err)
		}
	})
}

func TestStorageRouterOpenS3ObjectReaderRequiresKey(t *testing.T) {
	router := &storageRouter{s3: fakeS3ArchiveStore{}}
	_, _, err := router.openS3ObjectReader(context.Background(), locator.Ref{Kind: locator.KindS3, Raw: "s3://bucket/", Bucket: "bucket"})
	if err == nil || !strings.Contains(err.Error(), "s3 member key cannot be empty") {
		t.Fatalf("openS3ObjectReader() err = %v, want empty-key error", err)
	}
}

func TestStorageRouterDelegatesS3Operations(t *testing.T) {
	ctx := context.Background()
	ref := locator.Ref{Kind: locator.KindS3, Raw: "s3://bucket/key", Bucket: "bucket", Key: "key", CacheControl: "no-cache"}
	writer := &fakeWriteCloser{}
	var uploadBody string
	assertRef := func(label string, got locator.Ref) {
		t.Helper()
		if got.Kind != ref.Kind || got.Raw != ref.Raw || got.Bucket != ref.Bucket || got.Key != ref.Key || got.CacheControl != ref.CacheControl {
			t.Fatalf("%s ref = %+v, want %+v", label, got, ref)
		}
	}

	router := &storageRouter{
		s3: fakeS3ArchiveStore{
			openReader: func(_ context.Context, got locator.Ref) (io.ReadCloser, s3store.Metadata, error) {
				assertRef("OpenReader", got)
				return io.NopCloser(strings.NewReader("payload")), s3store.Metadata{Size: 7, ContentType: "application/gzip"}, nil
			},
			stat: func(_ context.Context, got locator.Ref) (s3store.Metadata, error) {
				assertRef("Stat", got)
				return s3store.Metadata{Size: 9}, nil
			},
			openWriter: func(_ context.Context, got locator.Ref, metadata map[string]string) (io.WriteCloser, error) {
				if got.Kind != ref.Kind || got.Raw != ref.Raw || got.Bucket != ref.Bucket || got.Key != ref.Key {
					t.Fatalf("OpenWriter ref = %+v, want bucket/key %s/%s", got, ref.Bucket, ref.Key)
				}
				if metadata["k"] != "v" {
					t.Fatalf("metadata = %+v", metadata)
				}
				return writer, nil
			},
			uploadStream: func(_ context.Context, got locator.Ref, body io.Reader, metadata map[string]string) error {
				assertRef("UploadStream", got)
				if metadata["m"] != "1" {
					t.Fatalf("upload metadata = %+v", metadata)
				}
				payload, err := io.ReadAll(body)
				if err != nil {
					return err
				}
				uploadBody = string(payload)
				return nil
			},
			listPrefix: func(_ context.Context, bucket string, prefix string) ([]s3store.ListedObject, error) {
				if bucket != "bucket" || prefix != "arch/" {
					t.Fatalf("ListPrefix(%q, %q)", bucket, prefix)
				}
				return []s3store.ListedObject{{Key: "arch/part0001", Size: 12}}, nil
			},
		},
	}

	rc, info, err := router.openArchiveReader(ctx, ref)
	if err != nil {
		t.Fatalf("openArchiveReader() error = %v", err)
	}
	defer rc.Close() //nolint:errcheck
	if info.Size != 7 || !info.SizeKnown || info.ContentType != "application/gzip" {
		t.Fatalf("archiveReaderInfo = %+v", info)
	}

	if _, err := router.openArchiveWriter(ctx, locator.Ref{Kind: locator.KindS3, Raw: ref.Raw, Bucket: ref.Bucket, Key: ref.Key, Metadata: map[string]string{"k": "v"}}); err != nil {
		t.Fatalf("openArchiveWriter() error = %v", err)
	}
	stat, err := router.statS3Object(ctx, ref)
	if err != nil || stat.Size != 9 {
		t.Fatalf("statS3Object() = %+v, %v", stat, err)
	}
	if err := router.uploadS3Object(ctx, ref, strings.NewReader("upload"), map[string]string{"m": "1"}); err != nil {
		t.Fatalf("uploadS3Object() error = %v", err)
	}
	if uploadBody != "upload" {
		t.Fatalf("upload body = %q, want upload", uploadBody)
	}
	objects, err := router.listS3Prefix(ctx, "bucket", "arch/")
	if err != nil {
		t.Fatalf("listS3Prefix() error = %v", err)
	}
	if len(objects) != 1 || objects[0].Key != "arch/part0001" {
		t.Fatalf("objects = %+v", objects)
	}
}

func TestStorageRouterPropagatesLocalReadErrors(t *testing.T) {
	wantErr := errors.New("disk read failed")
	router := &storageRouter{
		local: fakeLocalArchiveStore{
			openReader: func(ref locator.Ref) (io.ReadCloser, localstore.Metadata, error) {
				if ref.Path != "archive.tar" {
					t.Fatalf("ref.Path = %q", ref.Path)
				}
				return nil, localstore.Metadata{}, wantErr
			},
		},
	}

	_, _, err := router.openArchiveReader(context.Background(), locator.Ref{Kind: locator.KindLocal, Raw: "archive.tar", Path: "archive.tar"})
	if !errors.Is(err, wantErr) {
		t.Fatalf("openArchiveReader() err = %v, want %v", err, wantErr)
	}
}
