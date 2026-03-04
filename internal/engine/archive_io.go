package engine

import (
	"context"
	"fmt"
	"io"
	"maps"
	"strings"

	"github.com/islishude/gotgz/internal/locator"
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
	magic, replay, err := replayWithMagicPrefix(ar, 8)
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
	switch ref.Kind {
	case locator.KindLocal, locator.KindStdio:
		rc, meta, err := r.local.OpenReader(ref)
		if err != nil {
			return nil, archiveReaderInfo{}, err
		}
		return rc, archiveReaderInfo{Size: meta.Size, SizeKnown: ref.Kind == locator.KindLocal}, nil
	case locator.KindS3:
		if strings.TrimSpace(ref.Key) == "" {
			return nil, archiveReaderInfo{}, fmt.Errorf("archive object key cannot be empty for -f")
		}
		rc, meta, err := r.s3.OpenReader(ctx, ref)
		if err != nil {
			return nil, archiveReaderInfo{}, err
		}
		return rc, archiveReaderInfo{Size: meta.Size, SizeKnown: true, ContentType: strings.TrimSpace(meta.ContentType)}, nil
	case locator.KindHTTP:
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

// openArchiveWriter opens a write target for archive creation.
func (r *Runner) openArchiveWriter(ctx context.Context, ref locator.Ref) (io.WriteCloser, error) {
	switch ref.Kind {
	case locator.KindLocal, locator.KindStdio:
		return r.local.OpenWriter(ref)
	case locator.KindS3:
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

// mergeMetadata copies metadata maps with overlay keys taking precedence.
func mergeMetadata(base, overlay map[string]string) map[string]string {
	if len(base) == 0 && len(overlay) == 0 {
		return nil
	}
	out := make(map[string]string, len(base)+len(overlay))
	maps.Copy(out, base)
	maps.Copy(out, overlay)
	return out
}
