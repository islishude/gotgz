package engine

import (
	"archive/tar"
	"context"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/islishude/gotgz/packages/archive"
	"github.com/islishude/gotgz/packages/archiveprogress"
	"github.com/islishude/gotgz/packages/archiveutil"
	"github.com/islishude/gotgz/packages/locator"
)

// extractTarEntriesToS3Concurrent creates one bounded worker pool for tar->S3 uploads.
func (r *Runner) extractTarEntriesToS3Concurrent(ctx context.Context, target locator.Ref, scan func(pipeline *s3ExtractPipeline, budget *s3ExtractStagingBudget) (int, error)) (int, error) {
	pipeline := newS3ExtractPipeline(ctx, r.s3Extract.workers)
	budget := newS3ExtractStagingBudget(r.s3Extract.stagingBytes)
	warnings, err := scan(pipeline, budget)
	waitErr := pipeline.Wait()
	if err != nil {
		return warnings, err
	}
	return warnings, waitErr
}

// extractToS3Concurrent uploads one tar entry with staging when it improves throughput.
func (r *Runner) extractToS3Concurrent(ctx context.Context, target locator.Ref, hdr *tar.Header, tr *tar.Reader, reporter *archiveprogress.Reporter, pipeline *s3ExtractPipeline, budget *s3ExtractStagingBudget) (int, error) {
	warnings := 0
	name := strings.TrimPrefix(hdr.Name, "./")
	if name == "" {
		if hdr.Size > 0 {
			if _, err := archiveutil.CopyWithContext(ctx, io.Discard, io.LimitReader(tr, hdr.Size)); err != nil {
				return warnings, err
			}
		}
		return warnings, nil
	}

	meta, ok := archive.HeaderToS3Metadata(hdr)
	meta = archiveutil.MergeMetadata(target.Metadata, meta)
	if !ok {
		warnings += r.warnf(reporter, "metadata exceeds S3 metadata limit for %s", hdr.Name)
	}

	switch hdr.Typeflag {
	case tar.TypeReg:
		return warnings, r.extractRegularTarToS3Concurrent(ctx, target, name, hdr.Size, io.LimitReader(tr, hdr.Size), meta, pipeline, budget)
	case tar.TypeDir:
		if _, err := archiveutil.CopyWithContext(ctx, io.Discard, io.LimitReader(tr, hdr.Size)); err != nil {
			return warnings, err
		}
		return warnings, nil
	default:
		meta["gotgz-type"] = strconv.Itoa(int(hdr.Typeflag))
		return warnings, r.uploadToS3Target(ctx, target, name, strings.NewReader(""), meta)
	}
}

// extractRegularTarToS3Concurrent stages smaller tar entries before scheduling uploads.
func (r *Runner) extractRegularTarToS3Concurrent(ctx context.Context, target locator.Ref, name string, size int64, body io.Reader, meta map[string]string, pipeline *s3ExtractPipeline, budget *s3ExtractStagingBudget) error {
	if size == 0 {
		return pipeline.Submit(func(ctx context.Context) error {
			return r.uploadToS3Target(ctx, target, name, strings.NewReader(""), meta)
		})
	}
	if !budget.Fits(size) {
		return r.uploadToS3Target(pipeline.Context(), target, name, body, meta)
	}
	if err := budget.Acquire(pipeline.Context(), size); err != nil {
		return err
	}

	stagedPath, err := stageReaderToTempPath(pipeline.Context(), r.s3Extract.stagingDir, "gotgz-tar-s3-extract-*", body)
	if err != nil {
		budget.Release(size)
		return err
	}

	if err := pipeline.Submit(func(ctx context.Context) error {
		defer budget.Release(size)
		defer os.Remove(stagedPath) //nolint:errcheck

		file, err := openStagedFileReader(stagedPath)
		if err != nil {
			return err
		}
		defer file.Close() //nolint:errcheck
		return r.uploadToS3Target(ctx, target, name, file, meta)
	}); err != nil {
		budget.Release(size)
		_ = os.Remove(stagedPath)
		return err
	}
	return nil
}
