package engine

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/islishude/gotgz/packages/locator"
	s3store "github.com/islishude/gotgz/packages/storage/s3"
)

func TestResolveS3ArchiveVolumes(t *testing.T) {
	r := &Runner{storage: &storageRouter{s3: fakeS3ArchiveStore{listPrefix: func(_ context.Context, bucket, prefix string) ([]s3store.ListedObject, error) {
		if bucket != "mybucket" || prefix != "arch/bundle.part" {
			t.Fatalf("bucket=%q prefix=%q", bucket, prefix)
		}
		return []s3store.ListedObject{{Key: "arch/bundle.part0001.tar", Size: 100}, {Key: "arch/bundle.part0002.tar", Size: 200}, {Key: "arch/bundle.part0003.tar", Size: 300}}, nil
	}}}}

	volumes, err := r.resolveArchiveVolumes(context.Background(), locator.Ref{Kind: locator.KindS3, Raw: "s3://mybucket/arch/bundle.part0001.tar", Bucket: "mybucket", Key: "arch/bundle.part0001.tar"}, archiveReaderInfo{Size: 100, SizeKnown: true})
	if err != nil {
		t.Fatalf("resolveArchiveVolumes() error = %v", err)
	}
	if len(volumes) != 3 {
		t.Fatalf("len(volumes) = %d, want 3", len(volumes))
	}
	if volumes[2].info.Size != 300 {
		t.Fatalf("volumes[2].info.Size = %d, want 300", volumes[2].info.Size)
	}
}

func TestResolveS3ArchiveVolumesFailures(t *testing.T) {
	t.Run("gap", func(t *testing.T) {
		r := &Runner{storage: &storageRouter{s3: fakeS3ArchiveStore{listPrefix: func(_ context.Context, _, _ string) ([]s3store.ListedObject, error) {
			return []s3store.ListedObject{{Key: "arch/bundle.part0001.tar", Size: 100}, {Key: "arch/bundle.part0003.tar", Size: 300}}, nil
		}}}}
		_, err := r.resolveArchiveVolumes(context.Background(), locator.Ref{Kind: locator.KindS3, Raw: "s3://bucket/arch/bundle.part0001.tar", Bucket: "bucket", Key: "arch/bundle.part0001.tar"}, archiveReaderInfo{Size: 100, SizeKnown: true})
		if err == nil || !strings.Contains(err.Error(), "missing split archive volume") {
			t.Fatalf("err = %v, want missing volume error", err)
		}
	})

	t.Run("list error", func(t *testing.T) {
		wantErr := errors.New("list failed")
		r := &Runner{storage: &storageRouter{s3: fakeS3ArchiveStore{listPrefix: func(_ context.Context, _, _ string) ([]s3store.ListedObject, error) {
			return nil, wantErr
		}}}}
		_, err := r.resolveArchiveVolumes(context.Background(), locator.Ref{Kind: locator.KindS3, Raw: "s3://bucket/arch/bundle.part0001.tar", Bucket: "bucket", Key: "arch/bundle.part0001.tar"}, archiveReaderInfo{Size: 100, SizeKnown: true})
		if !errors.Is(err, wantErr) {
			t.Fatalf("err = %v, want %v", err, wantErr)
		}
	})

	t.Run("http split unsupported", func(t *testing.T) {
		r := &Runner{}
		_, err := r.resolveArchiveVolumes(context.Background(), locator.Ref{Kind: locator.KindHTTP, Raw: "https://example.test/bundle.part0001.tar", URL: "https://example.test/bundle.part0001.tar"}, archiveReaderInfo{Size: 100, SizeKnown: true})
		if err == nil || !strings.Contains(err.Error(), "http") {
			t.Fatalf("err = %v, want http split error", err)
		}
	})
}

func TestResolveArchiveVolumesNonSplitNameReturnsOneVolume(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bundle.tar")
	if err := os.WriteFile(path, []byte("payload"), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	volumes, err := (&Runner{}).resolveArchiveVolumes(context.Background(), locator.Ref{Kind: locator.KindLocal, Raw: path, Path: path}, archiveReaderInfo{Size: 7, SizeKnown: true})
	if err != nil {
		t.Fatalf("resolveArchiveVolumes() error = %v", err)
	}
	if len(volumes) != 1 {
		t.Fatalf("len(volumes) = %d, want 1", len(volumes))
	}
}

func TestResolveArchiveVolumesPartNotOneRejected(t *testing.T) {
	_, err := (&Runner{}).resolveArchiveVolumes(context.Background(), locator.Ref{Kind: locator.KindLocal, Raw: "bundle.part0002.tar", Path: "bundle.part0002.tar"}, archiveReaderInfo{Size: 10, SizeKnown: true})
	if err == nil || !strings.Contains(err.Error(), "part0001") {
		t.Fatalf("err = %v, want part0001 error", err)
	}
}
