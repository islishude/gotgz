package engine

import (
	"context"
	"io"

	"github.com/islishude/gotgz/packages/archiveutil"
	"github.com/islishude/gotgz/packages/locator"
)

// openArchiveForRead opens a readable archive stream and returns the stream
// plus a replayable magic-byte prefix used for archive format detection.
func (r *Runner) openArchiveForRead(ctx context.Context, archive string) (locator.Ref, io.ReadCloser, archiveReaderInfo, []byte, error) {
	ref, err := locator.ParseArchive(archive)
	if err != nil {
		return locator.Ref{}, nil, archiveReaderInfo{}, nil, err
	}
	ar, info, err := r.openArchiveReader(ctx, ref)
	if err != nil {
		return locator.Ref{}, nil, archiveReaderInfo{}, nil, err
	}
	magic, replay, err := archiveutil.ReplayWithMagicPrefix(ar, 8)
	if err != nil {
		_ = ar.Close()
		return locator.Ref{}, nil, archiveReaderInfo{}, nil, err
	}
	return ref, replay, info, magic, nil
}

// openArchiveReader opens the archive for reading and returns the reader along
// with metadata about the archive (size, whether the size is known, and the
// content type).
func (r *Runner) openArchiveReader(ctx context.Context, ref locator.Ref) (io.ReadCloser, archiveReaderInfo, error) {
	return r.storage.openArchiveReader(ctx, ref)
}

// openArchiveWriter opens a write target for archive creation.
func (r *Runner) openArchiveWriter(ctx context.Context, ref locator.Ref) (io.WriteCloser, error) {
	return r.storage.openArchiveWriter(ctx, ref)
}
