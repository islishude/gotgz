package engine

import (
	"context"
	"io"

	"github.com/islishude/gotgz/internal/locator"
	httpstore "github.com/islishude/gotgz/internal/storage/http"
	localstore "github.com/islishude/gotgz/internal/storage/local"
	s3store "github.com/islishude/gotgz/internal/storage/s3"
)

// localArchiveStore reads and writes archives on the local filesystem or stdio.
type localArchiveStore interface {
	OpenReader(ref locator.Ref) (io.ReadCloser, localstore.Metadata, error)
	OpenWriter(ref locator.Ref) (io.WriteCloser, error)
}

// s3ArchiveStore reads, writes, and enumerates archive objects in S3.
type s3ArchiveStore interface {
	OpenReader(ctx context.Context, ref locator.Ref) (io.ReadCloser, s3store.Metadata, error)
	Stat(ctx context.Context, ref locator.Ref) (s3store.Metadata, error)
	OpenWriter(ctx context.Context, ref locator.Ref, metadata map[string]string) (io.WriteCloser, error)
	UploadStream(ctx context.Context, ref locator.Ref, body io.Reader, metadata map[string]string) error
	ListPrefix(ctx context.Context, bucket string, prefix string) ([]s3store.ListedObject, error)
}

// httpArchiveStore opens archive sources over HTTP(S).
type httpArchiveStore interface {
	OpenReader(ctx context.Context, ref locator.Ref) (io.ReadCloser, httpstore.Metadata, error)
}
