package engine

import (
	"archive/zip"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/islishude/gotgz/packages/archiveprogress"
	"github.com/islishude/gotgz/packages/archiveutil"
	"github.com/islishude/gotgz/packages/locator"
)

const (
	maxZipSymlinkTargetBytes    = 4096
	defaultZipStagingLimitBytes = 1 << 30
	zipStagingLimitEnv          = "GOTGZ_ZIP_STAGING_LIMIT_BYTES"
)

// withZipReader opens a zip.Reader from local file directly when possible and
// otherwise prefers remote range reads before copying source bytes to a
// temporary file to satisfy ReaderAt. progressReporter tracks archive bytes
// consumed while preparing the zip reader.
func (r *Runner) withZipReader(ctx context.Context, archiveRef locator.Ref, ar io.ReadCloser, info archiveReaderInfo, progressReporter *archiveprogress.Reporter, fn func(zr *zip.Reader) (int, error)) (int, error) {
	if archiveRef.Kind == locator.KindLocal && info.SizeKnown && archiveRef.Path != "" {
		f, err := os.Open(archiveRef.Path)
		if err == nil {
			defer f.Close() //nolint:errcheck
			st, statErr := f.Stat()
			if statErr == nil && st.Mode().IsRegular() {
				zr, zipErr := zip.NewReader(f, st.Size())
				if zipErr == nil {
					if progressReporter != nil {
						progressReporter.AddDone(st.Size())
					}
					return fn(zr)
				}
			}
		}
	}

	if zr, err := r.tryRemoteZipReader(ctx, archiveRef, ar, info, progressReporter); zr != nil && err == nil {
		return fn(zr)
	}

	enforceStagingLimit := zipStagingLimitApplies(archiveRef)
	stagingLimit := zipStagingLimitBytes()
	if enforceStagingLimit && info.SizeKnown && info.Size > stagingLimit {
		return 0, zipStagingLimitError(archiveRef, stagingLimit)
	}

	tmp, err := os.CreateTemp("", "gotgz-zip-*")
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
	}()

	copySrc := io.Reader(ar)
	if progressReporter != nil {
		copySrc = archiveprogress.NewCountingReader(ar, progressReporter)
	}
	if enforceStagingLimit {
		if _, err := archiveutil.CopyWithContextLimit(ctx, tmp, copySrc, stagingLimit); err != nil {
			if errors.Is(err, archiveutil.ErrCopyLimitExceeded) {
				return 0, zipStagingLimitError(archiveRef, stagingLimit)
			}
			return 0, err
		}
	} else if _, err := archiveutil.CopyWithContext(ctx, tmp, copySrc); err != nil {
		return 0, err
	}
	st, err := tmp.Stat()
	if err != nil {
		return 0, err
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}
	zr, err := zip.NewReader(tmp, st.Size())
	if err != nil {
		return 0, err
	}
	return fn(zr)
}

// tryRemoteZipReader opens a zip.Reader backed by remote range requests when
// the archive source supports random access and the total size is known.
func (r *Runner) tryRemoteZipReader(ctx context.Context, archiveRef locator.Ref, ar io.ReadCloser, info archiveReaderInfo, progressReporter *archiveprogress.Reporter) (*zip.Reader, error) {
	if !info.SizeKnown {
		return nil, nil
	}
	if archiveRef.Kind != locator.KindS3 && archiveRef.Kind != locator.KindHTTP {
		return nil, nil
	}

	readerAt := newRemoteZipReaderAt(ctx, info.Size, defaultRemoteZipReadBlockSize, func(ctx context.Context, offset int64, length int64) (io.ReadCloser, error) {
		rc, err := r.storage.openZipRangeReader(ctx, archiveRef, offset, length)
		if err != nil {
			return nil, err
		}
		return archiveprogress.NewCountingReadCloser(rc, progressReporter), nil
	})
	zr, err := zip.NewReader(readerAt, info.Size)
	if err != nil {
		return nil, err
	}
	if ar != nil {
		_ = ar.Close()
	}
	return zr, nil
}

// zipStagingLimitApplies reports whether staged ZIP bytes should be capped for
// this archive source kind.
func zipStagingLimitApplies(ref locator.Ref) bool {
	return ref.Kind != locator.KindLocal
}

// zipStagingLimitBytes returns the maximum number of bytes a non-local zip
// source may spool to temporary storage before being rejected.
func zipStagingLimitBytes() int64 {
	v := strings.TrimSpace(os.Getenv(zipStagingLimitEnv))
	if v == "" {
		return defaultZipStagingLimitBytes
	}
	limit, err := strconv.ParseInt(v, 10, 64)
	if err != nil || limit <= 0 {
		return defaultZipStagingLimitBytes
	}
	return limit
}

// zipStagingLimitError formats the staging-limit failure for user-facing
// extract/list errors.
func zipStagingLimitError(ref locator.Ref, limit int64) error {
	source := ref.Raw
	if strings.TrimSpace(source) == "" {
		source = ref.Path
	}
	if strings.TrimSpace(source) == "" {
		source = ref.URL
	}
	if strings.TrimSpace(source) == "" {
		source = "zip input"
	}
	return fmt.Errorf("%s exceeds zip staging limit of %d bytes; set %s to raise the limit", source, limit, zipStagingLimitEnv)
}
