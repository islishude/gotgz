package engine

import (
	"context"
	"io"

	"github.com/islishude/gotgz/internal/locator"
)

type Metadata struct {
	Size int64
}

type ObjectReader interface {
	Open(ctx context.Context, ref locator.Ref) (io.ReadCloser, Metadata, error)
}

type ObjectWriter interface {
	Create(ctx context.Context, ref locator.Ref, metadata map[string]string) (io.WriteCloser, error)
}

type MemberSource interface {
	Stat(ctx context.Context, ref locator.Ref) (Metadata, error)
	Open(ctx context.Context, ref locator.Ref) (io.ReadCloser, error)
}

type MemberSink interface {
	Mkdir(ctx context.Context, ref locator.Ref, mode uint32) error
	WriteFile(ctx context.Context, ref locator.Ref, body io.Reader, metadata map[string]string) error
	Symlink(ctx context.Context, ref locator.Ref, target string, metadata map[string]string) error
	Hardlink(ctx context.Context, ref locator.Ref, target locator.Ref, metadata map[string]string) error
}
