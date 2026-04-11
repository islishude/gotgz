package engine

import (
	"context"
	"io"

	"github.com/islishude/gotgz/packages/locator"
	httpstore "github.com/islishude/gotgz/packages/storage/http"
	localstore "github.com/islishude/gotgz/packages/storage/local"
	s3store "github.com/islishude/gotgz/packages/storage/s3"
)

// localArchiveStore reads and writes archives on the local filesystem or stdio.
type localArchiveStore interface {
	OpenReader(ref locator.Ref) (io.ReadCloser, localstore.Metadata, error)
	OpenWriter(ref locator.Ref) (io.WriteCloser, error)
}

// zipArchiveRangeStore opens exact byte ranges from remote archives so ZIP
// reads can satisfy io.ReaderAt without staging the full archive stream.
//
// ZIP archives store a central directory at the end of the file, so the reader
// must seek to arbitrary offsets (Go's archive/zip requires io.ReaderAt). TAR
// archives are purely sequential and only need io.Reader, so they stream
// directly without range requests.
type zipArchiveRangeStore interface {
	OpenRangeReader(ctx context.Context, ref locator.Ref, offset int64, length int64) (io.ReadCloser, error)
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
