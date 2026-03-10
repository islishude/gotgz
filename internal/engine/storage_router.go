package engine

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/islishude/gotgz/internal/locator"
	localstore "github.com/islishude/gotgz/internal/storage/local"
	s3store "github.com/islishude/gotgz/internal/storage/s3"
)

// storageRouter centralizes backend-specific archive and object operations.
type storageRouter struct {
	local localArchiveStore
	s3    s3ArchiveStore
	http  httpArchiveStore
}

func (r *storageRouter) requireLocal() error {
	if r.local == nil {
		return fmt.Errorf("local archive store is not configured")
	}
	return nil
}

func (r *storageRouter) requireS3() error {
	if r.s3 == nil {
		return fmt.Errorf("s3 archive store is not configured")
	}
	return nil
}

func (r *storageRouter) requireHTTP() error {
	if r.http == nil {
		return fmt.Errorf("http archive store is not configured")
	}
	return nil
}

// openArchiveReader resolves an archive source and returns its stream plus metadata.
func (r *storageRouter) openArchiveReader(ctx context.Context, ref locator.Ref) (io.ReadCloser, archiveReaderInfo, error) {
	switch ref.Kind {
	case locator.KindLocal, locator.KindStdio:
		if err := r.requireLocal(); err != nil {
			return nil, archiveReaderInfo{}, err
		}
		rc, meta, err := r.local.OpenReader(ref)
		if err != nil {
			return nil, archiveReaderInfo{}, err
		}
		return rc, archiveReaderInfo{Size: meta.Size, SizeKnown: ref.Kind == locator.KindLocal}, nil
	case locator.KindS3:
		return r.openS3ArchiveReader(ctx, ref)
	case locator.KindHTTP:
		if err := r.requireHTTP(); err != nil {
			return nil, archiveReaderInfo{}, err
		}
		rc, meta, err := r.http.OpenReader(ctx, ref)
		if err != nil {
			return nil, archiveReaderInfo{}, err
		}
		if meta.Size >= 0 {
			return rc, archiveReaderInfo{Size: meta.Size, SizeKnown: true, ContentType: meta.ContentType}, nil
		}
		return rc, archiveReaderInfo{ContentType: meta.ContentType}, nil
	default:
		return nil, archiveReaderInfo{}, fmt.Errorf("unsupported archive source %q", ref.Raw)
	}
}

// openArchiveWriter resolves an archive target and opens it for writes.
func (r *storageRouter) openArchiveWriter(ctx context.Context, ref locator.Ref) (io.WriteCloser, error) {
	switch ref.Kind {
	case locator.KindLocal, locator.KindStdio:
		if err := r.requireLocal(); err != nil {
			return nil, err
		}
		return r.local.OpenWriter(ref)
	case locator.KindS3:
		if err := r.requireS3(); err != nil {
			return nil, err
		}
		if strings.TrimSpace(ref.Key) == "" {
			return nil, fmt.Errorf("archive object key cannot be empty for -f")
		}
		return r.s3.OpenWriter(ctx, ref, ref.Metadata)
	case locator.KindHTTP:
		return nil, fmt.Errorf("unsupported archive target %q: http(s) archives are source-only", ref.Raw)
	default:
		return nil, fmt.Errorf("unsupported archive target %q", ref.Raw)
	}
}

// openS3ObjectReader opens one S3 object as a generic member stream.
func (r *storageRouter) openS3ObjectReader(ctx context.Context, ref locator.Ref) (io.ReadCloser, s3store.Metadata, error) {
	if err := r.requireS3(); err != nil {
		return nil, s3store.Metadata{}, err
	}
	if strings.TrimSpace(ref.Key) == "" {
		return nil, s3store.Metadata{}, fmt.Errorf("s3 member key cannot be empty: %q", ref.Raw)
	}
	return r.s3.OpenReader(ctx, ref)
}

// statS3Object returns metadata for one S3 object member.
func (r *storageRouter) statS3Object(ctx context.Context, ref locator.Ref) (s3store.Metadata, error) {
	if err := r.requireS3(); err != nil {
		return s3store.Metadata{}, err
	}
	return r.s3.Stat(ctx, ref)
}

// uploadS3Object writes one object payload into S3.
func (r *storageRouter) uploadS3Object(ctx context.Context, ref locator.Ref, body io.Reader, metadata map[string]string) error {
	if err := r.requireS3(); err != nil {
		return err
	}
	return r.s3.UploadStream(ctx, ref, body, metadata)
}

// listS3Prefix enumerates S3 objects matching one prefix.
func (r *storageRouter) listS3Prefix(ctx context.Context, bucket string, prefix string) ([]s3store.ListedObject, error) {
	if err := r.requireS3(); err != nil {
		return nil, err
	}
	return r.s3.ListPrefix(ctx, bucket, prefix)
}

// openS3ArchiveReader opens an S3 archive object and maps its metadata for archive reads.
func (r *storageRouter) openS3ArchiveReader(ctx context.Context, ref locator.Ref) (io.ReadCloser, archiveReaderInfo, error) {
	if err := r.requireS3(); err != nil {
		return nil, archiveReaderInfo{}, err
	}
	if strings.TrimSpace(ref.Key) == "" {
		return nil, archiveReaderInfo{}, fmt.Errorf("archive object key cannot be empty for -f")
	}
	rc, meta, err := r.s3.OpenReader(ctx, ref)
	if err != nil {
		return nil, archiveReaderInfo{}, err
	}
	return rc, archiveReaderInfo{Size: meta.Size, SizeKnown: true, ContentType: strings.TrimSpace(meta.ContentType)}, nil
}

var _ localArchiveStore = (*localstore.ArchiveStore)(nil)
