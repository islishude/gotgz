package engine

import (
	"archive/zip"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/islishude/gotgz/packages/archivepath"
	"github.com/islishude/gotgz/packages/archiveprogress"
	"github.com/islishude/gotgz/packages/archiveutil"
	"github.com/islishude/gotgz/packages/cli"
	"github.com/islishude/gotgz/packages/locator"
)

// shouldUseConcurrentS3Extract reports whether this run should enable object-level
// concurrency for archive extraction into S3.
func (r *Runner) shouldUseConcurrentS3Extract(target locator.Ref) bool {
	return target.Kind == locator.KindS3 && r.s3Extract.workers > 1
}

// withConcurrentS3ZipReader opens a zip.Reader suitable for concurrent entry uploads.
func (r *Runner) withConcurrentS3ZipReader(ctx context.Context, archiveRef locator.Ref, ar io.ReadCloser, info archiveReaderInfo, _ *archiveprogress.Reporter, fn func(zr *zip.Reader) (int, error)) (int, error) {
	if archiveRef.Kind == locator.KindLocal && info.SizeKnown && archiveRef.Path != "" {
		f, err := os.Open(archiveRef.Path)
		if err == nil {
			defer f.Close() //nolint:errcheck
			st, statErr := f.Stat()
			if statErr == nil && st.Mode().IsRegular() {
				zr, zipErr := zip.NewReader(f, st.Size())
				if zipErr == nil {
					return fn(zr)
				}
			}
		}
	}

	enforceStagingLimit := zipStagingLimitApplies(archiveRef)
	stagingLimit := zipStagingLimitBytes()
	if enforceStagingLimit && info.SizeKnown && info.Size > stagingLimit {
		return 0, zipStagingLimitError(archiveRef, stagingLimit)
	}

	limit := int64(-1)
	if enforceStagingLimit {
		limit = stagingLimit
	}
	staged, size, err := stageReaderToTempFileLimit(ctx, r.s3Extract.stagingDir, "gotgz-zip-s3-extract-*", ar, limit)
	if err != nil {
		if errors.Is(err, archiveutil.ErrCopyLimitExceeded) {
			return 0, zipStagingLimitError(archiveRef, stagingLimit)
		}
		return 0, err
	}
	defer func() {
		_ = cleanupTempFile(staged)
	}()

	if enforceStagingLimit && size > stagingLimit {
		return 0, zipStagingLimitError(archiveRef, stagingLimit)
	}

	zr, err := zip.NewReader(staged, size)
	if err != nil {
		return 0, err
	}
	return fn(zr)
}

// extractZipEntriesToS3Concurrent uploads matching zip entries to S3 with one worker pool.
func (r *Runner) extractZipEntriesToS3Concurrent(ctx context.Context, zr *zip.Reader, opts cli.Options, reporter *archiveprogress.Reporter, target locator.Ref, memberMatcher *archivepath.CompiledPathMatcher) (int, error) {
	pipeline := newS3ExtractPipeline(ctx, r.s3Extract.workers)
	warnings := 0

	for _, zf := range zr.File {
		select {
		case <-pipeline.Context().Done():
			return warnings, pipeline.Wait()
		default:
		}
		if archivepath.ShouldSkipMemberWithMatcher(memberMatcher, zf.Name) {
			continue
		}
		extractName, ok := archivepath.StripPathComponents(zf.Name, opts.StripComponents)
		if !ok || extractName == "" {
			continue
		}
		if opts.Verbose {
			reporter.BeforeExternalLineOutput()
			_, _ = fmt.Fprintln(r.stdout, extractName)
			reporter.AfterExternalLineOutput()
		}

		name := strings.TrimPrefix(extractName, "./")
		if name == "" || isZipDir(zf) {
			continue
		}
		if !isZipRegular(zf) && !isZipSymlink(zf) {
			warnings += r.warnf(reporter, "zip entry %s has unsupported type %s on S3 target; skipping", zf.Name, zf.Mode().String())
			continue
		}
		if isZipSymlink(zf) {
			warnings += r.warnf(reporter, "zip symlink %s extracted to S3 as regular object", zf.Name)
		}
		skip, w, err := r.probeZipEntry(zf, reporter)
		warnings += w
		if err != nil {
			waitErr := pipeline.Wait()
			if waitErr != nil {
				return warnings, waitErr
			}
			return warnings, err
		}
		if skip {
			continue
		}

		entry := zf
		keyName := name
		if err := pipeline.Submit(func(ctx context.Context) error {
			rc, err := entry.Open()
			if err != nil {
				return err
			}
			defer rc.Close() //nolint:errcheck
			return r.uploadToS3Target(ctx, target, keyName, archiveprogress.NewCountingReader(rc, reporter), target.Metadata)
		}); err != nil {
			waitErr := pipeline.Wait()
			if waitErr != nil {
				return warnings, waitErr
			}
			return warnings, err
		}
	}

	return warnings, pipeline.Wait()
}

// probeZipEntry validates that one zip entry can be opened before the worker pool
// emits verbose output and deterministic warnings in archive order.
func (r *Runner) probeZipEntry(zf *zip.File, reporter *archiveprogress.Reporter) (bool, int, error) {
	rc, err := zf.Open()
	if err == nil {
		return false, 0, rc.Close()
	}
	if errors.Is(err, zip.ErrAlgorithm) {
		return true, r.warnf(reporter, "zip entry %s uses unsupported algorithm/encryption; skipping", zf.Name), nil
	}
	return false, 0, err
}
