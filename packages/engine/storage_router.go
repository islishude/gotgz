package engine

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"

	"github.com/islishude/gotgz/packages/archiveutil"
	"github.com/islishude/gotgz/packages/locator"
	localstore "github.com/islishude/gotgz/packages/storage/local"
	s3store "github.com/islishude/gotgz/packages/storage/s3"
)

// storageRouter centralizes backend-specific archive and object operations.
type storageRouter struct {
	local                localArchiveStore
	s3                   s3ArchiveStore
	s3Factory            func(context.Context) (s3ArchiveStore, error)
	s3Once               sync.Once
	s3Err                error
	s3ZipRange           zipArchiveRangeStore
	s3SnapshotZipRange   snapshotZipArchiveRangeStore
	http                 httpArchiveStore
	httpZipRange         zipArchiveRangeStore
	httpSnapshotZipRange snapshotZipArchiveRangeStore
}

func newStorageRouterWithS3Factory(local localArchiveStore, factory func(context.Context) (s3ArchiveStore, error), http httpArchiveStore) *storageRouter {
	router := newStorageRouter(local, nil, http)
	router.s3Factory = factory
	return router
}

// newStorageRouter builds one storage router and wires optional ZIP-specific
// range readers only for backends that expose them.
func newStorageRouter(local localArchiveStore, s3 s3ArchiveStore, http httpArchiveStore) *storageRouter {
	router := &storageRouter{
		local: local,
		s3:    s3,
		http:  http,
	}
	if rangeStore, ok := s3.(zipArchiveRangeStore); ok {
		router.s3ZipRange = rangeStore
	}
	if rangeStore, ok := s3.(snapshotZipArchiveRangeStore); ok {
		router.s3SnapshotZipRange = rangeStore
	}
	if rangeStore, ok := http.(zipArchiveRangeStore); ok {
		router.httpZipRange = rangeStore
	}
	if rangeStore, ok := http.(snapshotZipArchiveRangeStore); ok {
		router.httpSnapshotZipRange = rangeStore
	}
	return router
}

func (r *storageRouter) requireLocal() error {
	if r.local == nil {
		return fmt.Errorf("local archive store is not configured")
	}
	return nil
}

func (r *storageRouter) requireS3(ctx context.Context) (s3ArchiveStore, error) {
	if r.s3Factory == nil {
		if r.s3 == nil {
			return nil, fmt.Errorf("s3 archive store is not configured")
		}
		return r.s3, nil
	}
	r.s3Once.Do(func() {
		r.s3, r.s3Err = r.s3Factory(ctx)
		if rangeStore, ok := r.s3.(zipArchiveRangeStore); ok {
			r.s3ZipRange = rangeStore
		}
		if rangeStore, ok := r.s3.(snapshotZipArchiveRangeStore); ok {
			r.s3SnapshotZipRange = rangeStore
		}
	})
	if r.s3Err != nil {
		return nil, r.s3Err
	}
	if r.s3 == nil {
		return nil, fmt.Errorf("s3 archive store is not configured")
	}
	return r.s3, nil
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
		snapshot := archiveutil.Snapshot{Size: meta.Size, ETag: meta.ETag, LastModified: meta.LastModified, URL: meta.URL}
		if meta.Size >= 0 {
			return rc, archiveReaderInfo{Size: meta.Size, SizeKnown: true, ContentType: meta.ContentType, Snapshot: snapshot}, nil
		}
		return rc, archiveReaderInfo{ContentType: meta.ContentType, Snapshot: snapshot}, nil
	default:
		return nil, archiveReaderInfo{}, fmt.Errorf("unsupported archive source %q", ref.Raw)
	}
}

// beginArchiveWriter opens a destination whose publication is controlled by
// explicit Commit or Abort calls.
func (r *storageRouter) beginArchiveWriter(ctx context.Context, ref locator.Ref) (archiveWriteSession, error) {
	switch ref.Kind {
	case locator.KindLocal, locator.KindStdio:
		if err := r.requireLocal(); err != nil {
			return nil, err
		}
		if store, ok := r.local.(transactionalLocalArchiveStore); ok {
			return store.BeginWriter(ref)
		}
		writer, err := r.local.OpenWriter(ref)
		if err != nil {
			return nil, err
		}
		return &legacyArchiveWriteSession{writer: writer}, nil
	case locator.KindS3:
		store, err := r.requireS3(ctx)
		if err != nil {
			return nil, err
		}
		if strings.TrimSpace(ref.Key) == "" {
			return nil, fmt.Errorf("archive object key cannot be empty for -f")
		}
		if transactional, ok := store.(transactionalS3ArchiveStore); ok {
			return transactional.BeginWriter(ctx, ref, ref.Metadata)
		}
		writer, err := store.OpenWriter(ctx, ref, ref.Metadata)
		if err != nil {
			return nil, err
		}
		return &legacyArchiveWriteSession{writer: writer}, nil
	case locator.KindHTTP:
		return nil, fmt.Errorf("unsupported archive target %q: http(s) archives are source-only", ref.Raw)
	default:
		return nil, fmt.Errorf("unsupported archive target %q", ref.Raw)
	}
}

type legacyArchiveWriteSession struct {
	writer io.WriteCloser
	once   sync.Once
	err    error
}

func (s *legacyArchiveWriteSession) Write(p []byte) (int, error) { return s.writer.Write(p) }
func (s *legacyArchiveWriteSession) Commit() error {
	s.once.Do(func() { s.err = s.writer.Close() })
	return s.err
}
func (s *legacyArchiveWriteSession) Abort(_ error) error {
	s.once.Do(func() { s.err = s.writer.Close() })
	return s.err
}

// openZipRangeReader opens one byte range from a remote archive source for ZIP
// random access reads.
func (r *storageRouter) openZipRangeReader(ctx context.Context, ref locator.Ref, offset int64, length int64, snapshots ...archiveutil.Snapshot) (io.ReadCloser, error) {
	var snapshot archiveutil.Snapshot
	if len(snapshots) > 0 {
		snapshot = snapshots[0]
	}
	switch ref.Kind {
	case locator.KindS3:
		if _, err := r.requireS3(ctx); err != nil {
			return nil, err
		}
		if r.s3ZipRange == nil {
			return nil, fmt.Errorf("s3 zip range store is not configured")
		}
		if strings.TrimSpace(ref.Key) == "" {
			return nil, fmt.Errorf("archive object key cannot be empty for -f")
		}
		if r.s3SnapshotZipRange != nil {
			return r.s3SnapshotZipRange.OpenRangeReaderSnapshot(ctx, ref, offset, length, snapshot)
		}
		return r.s3ZipRange.OpenRangeReader(ctx, ref, offset, length)
	case locator.KindHTTP:
		if r.httpZipRange == nil {
			return nil, fmt.Errorf("http zip range store is not configured")
		}
		if r.httpSnapshotZipRange != nil {
			return r.httpSnapshotZipRange.OpenRangeReaderSnapshot(ctx, ref, offset, length, snapshot)
		}
		return r.httpZipRange.OpenRangeReader(ctx, ref, offset, length)
	default:
		return nil, fmt.Errorf("unsupported zip range source %q", ref.Raw)
	}
}

// openS3ObjectReader opens one S3 object as a generic member stream.
func (r *storageRouter) openS3ObjectReader(ctx context.Context, ref locator.Ref) (io.ReadCloser, s3store.Metadata, error) {
	store, err := r.requireS3(ctx)
	if err != nil {
		return nil, s3store.Metadata{}, err
	}
	if strings.TrimSpace(ref.Key) == "" {
		return nil, s3store.Metadata{}, fmt.Errorf("s3 member key cannot be empty: %q", ref.Raw)
	}
	return store.OpenReader(ctx, ref)
}

// statS3Object returns metadata for one S3 object member.
func (r *storageRouter) statS3Object(ctx context.Context, ref locator.Ref) (s3store.Metadata, error) {
	store, err := r.requireS3(ctx)
	if err != nil {
		return s3store.Metadata{}, err
	}
	return store.Stat(ctx, ref)
}

// uploadS3Object writes one object payload into S3.
func (r *storageRouter) uploadS3Object(ctx context.Context, ref locator.Ref, body io.Reader, metadata map[string]string) error {
	store, err := r.requireS3(ctx)
	if err != nil {
		return err
	}
	return store.UploadStream(ctx, ref, body, metadata)
}

// listS3Prefix enumerates S3 objects matching one prefix.
func (r *storageRouter) listS3Prefix(ctx context.Context, bucket string, prefix string) ([]s3store.ListedObject, error) {
	store, err := r.requireS3(ctx)
	if err != nil {
		return nil, err
	}
	return store.ListPrefix(ctx, bucket, prefix)
}

// openS3ArchiveReader opens an S3 archive object and maps its metadata for archive reads.
func (r *storageRouter) openS3ArchiveReader(ctx context.Context, ref locator.Ref) (io.ReadCloser, archiveReaderInfo, error) {
	store, err := r.requireS3(ctx)
	if err != nil {
		return nil, archiveReaderInfo{}, err
	}
	if strings.TrimSpace(ref.Key) == "" {
		return nil, archiveReaderInfo{}, fmt.Errorf("archive object key cannot be empty for -f")
	}
	rc, meta, err := store.OpenReader(ctx, ref)
	if err != nil {
		return nil, archiveReaderInfo{}, err
	}
	snapshot := archiveutil.Snapshot{
		Size:      meta.Size,
		ETag:      meta.ETag,
		VersionID: meta.VersionID,
	}
	if !meta.LastModified.IsZero() {
		snapshot.LastModified = meta.LastModified.UTC().Format(http.TimeFormat)
	}
	return rc, archiveReaderInfo{Size: meta.Size, SizeKnown: true, ContentType: strings.TrimSpace(meta.ContentType), Snapshot: snapshot}, nil
}

var _ localArchiveStore = (*localstore.ArchiveStore)(nil)
