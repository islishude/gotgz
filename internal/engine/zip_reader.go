package engine

import (
	"archive/zip"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"strings"

	"github.com/islishude/gotgz/internal/cli"
	"github.com/islishude/gotgz/internal/locator"
)

const (
	maxZipSymlinkTargetBytes    = 4096
	defaultZipStagingLimitBytes = 1 << 30
	zipStagingLimitEnv          = "GOTGZ_ZIP_STAGING_LIMIT_BYTES"
)

// withZipReader opens a zip.Reader from local file directly when possible and
// otherwise prefers remote range reads before copying source bytes to a
// temporary file to satisfy ReaderAt. copyReporter tracks archive bytes
// consumed while preparing the zip reader.
func (r *Runner) withZipReader(ctx context.Context, archiveRef locator.Ref, ar io.ReadCloser, info archiveReaderInfo, copyReporter *progressReporter, fn func(zr *zip.Reader) (int, error)) (int, error) {
	if archiveRef.Kind == locator.KindLocal && info.SizeKnown && archiveRef.Path != "" {
		f, err := os.Open(archiveRef.Path)
		if err == nil {
			defer f.Close() //nolint:errcheck
			st, statErr := f.Stat()
			if statErr == nil && st.Mode().IsRegular() {
				zr, zipErr := zip.NewReader(f, st.Size())
				if zipErr == nil {
					if copyReporter != nil {
						copyReporter.AddDone(st.Size())
					}
					return fn(zr)
				}
			}
		}
	}

	if zr, err := r.tryRemoteZipReader(ctx, archiveRef, ar, info); zr != nil && err == nil {
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
	if copyReporter != nil {
		copySrc = newCountingReader(ar, copyReporter)
	}
	if enforceStagingLimit {
		if _, err := copyWithContextLimit(ctx, tmp, copySrc, stagingLimit); err != nil {
			if errors.Is(err, errCopyLimitExceeded) {
				return 0, zipStagingLimitError(archiveRef, stagingLimit)
			}
			return 0, err
		}
	} else if _, err := copyWithContext(ctx, tmp, copySrc); err != nil {
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
func (r *Runner) tryRemoteZipReader(ctx context.Context, archiveRef locator.Ref, ar io.ReadCloser, info archiveReaderInfo) (*zip.Reader, error) {
	if !info.SizeKnown {
		return nil, nil
	}
	if archiveRef.Kind != locator.KindS3 && archiveRef.Kind != locator.KindHTTP {
		return nil, nil
	}

	readerAt := newRemoteZipReaderAt(ctx, info.Size, defaultRemoteZipReadBlockSize, func(ctx context.Context, offset int64, length int64) (io.ReadCloser, error) {
		return r.storage.openArchiveRangeReader(ctx, archiveRef, offset, length)
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

// extractZipToStdout writes matching regular zip members to stdout.
func (r *Runner) extractZipToStdout(ctx context.Context, zr *zip.Reader, opts cli.Options, reporter *progressReporter) (int, error) {
	warnings := 0
	for _, zf := range zr.File {
		select {
		case <-ctx.Done():
			return warnings, ctx.Err()
		default:
		}
		if shouldSkipMember(opts, zf.Name) {
			continue
		}
		name, ok := stripPathComponents(zf.Name, opts.StripComponents)
		if !ok || name == "" || !isZipRegular(zf) {
			continue
		}
		rc, w, err := r.openZipEntry(zf, reporter)
		warnings += w
		if err != nil {
			return warnings, err
		}
		if rc == nil {
			continue
		}
		_, err = copyWithContext(ctx, r.stdout, newCountingReader(rc, reporter))
		cerr := rc.Close()
		if err != nil {
			return warnings, err
		}
		if cerr != nil {
			return warnings, cerr
		}
	}
	return warnings, nil
}

// openZipEntry opens one zip file entry and downgrades unsupported algorithms
// into warnings so extraction/list can continue.
func (r *Runner) openZipEntry(zf *zip.File, reporter *progressReporter) (io.ReadCloser, int, error) {
	rc, err := zf.Open()
	if err == nil {
		return rc, 0, nil
	}
	if errors.Is(err, zip.ErrAlgorithm) {
		return nil, r.warnf(reporter, "zip entry %s uses unsupported algorithm/encryption; skipping", zf.Name), nil
	}
	return nil, 0, err
}

// totalZipPayloadBytes sums uncompressed payload bytes for matching entries.
func totalZipPayloadBytes(zr *zip.Reader, match func(zf *zip.File) bool) int64 {
	var total uint64
	for _, zf := range zr.File {
		if match != nil && !match(zf) {
			continue
		}
		if isZipDir(zf) {
			continue
		}
		total += zf.UncompressedSize64
	}
	if total > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(total)
}

// isZipDir reports whether a zip entry is a directory.
func isZipDir(zf *zip.File) bool {
	if zf == nil {
		return false
	}
	if strings.HasSuffix(zf.Name, "/") {
		return true
	}
	return zf.FileInfo().IsDir()
}

// isZipSymlink reports whether a zip entry is a symbolic link.
func isZipSymlink(zf *zip.File) bool {
	if zf == nil {
		return false
	}
	return zf.Mode()&os.ModeSymlink != 0
}

// isZipRegular reports whether a zip entry should be treated as a regular file.
func isZipRegular(zf *zip.File) bool {
	if zf == nil || isZipDir(zf) || isZipSymlink(zf) {
		return false
	}
	return zf.Mode().IsRegular()
}

// readZipSymlinkTarget reads a symlink target from a zip entry with a hard cap
// to avoid unbounded memory growth on malformed archives.
func readZipSymlinkTarget(zf *zip.File, rc io.Reader, reporter *progressReporter) (string, error) {
	b, err := io.ReadAll(io.LimitReader(newCountingReader(rc, reporter), maxZipSymlinkTargetBytes+1))
	if err != nil {
		return "", err
	}
	if len(b) > maxZipSymlinkTargetBytes {
		return "", fmt.Errorf("zip symlink %s target exceeds %d bytes", zf.Name, maxZipSymlinkTargetBytes)
	}
	return string(b), nil
}
