package engine

import (
	"archive/zip"
	"context"
	"io"

	"github.com/islishude/gotgz/packages/archivepath"
	"github.com/islishude/gotgz/packages/archiveprogress"
	"github.com/islishude/gotgz/packages/cli"
	"github.com/islishude/gotgz/packages/locator"
)

// runListZip lists archive members from a zip input stream.
func (r *Runner) runListZip(ctx context.Context, opts cli.Options, reporter *archiveprogress.Reporter, archiveRef locator.Ref, ar io.ReadCloser, info archiveReaderInfo) (int, error) {
	warnings := r.warnZipReadOptions(opts, reporter)
	memberMatcher := archivepath.NewMemberMatcher(opts.Members, opts.Wildcards)
	volumes, err := r.resolveArchiveVolumes(ctx, archiveRef, info)
	if err != nil {
		return warnings, err
	}

	if len(volumes) == 1 {
		reporter.SetTotal(info.Size, info.SizeKnown)
		zipWarnings, err := r.withZipReader(ctx, archiveRef, ar, info, reporter, func(zr *zip.Reader) (int, error) {
			return r.listZipReader(ctx, zr, memberMatcher, reporter)
		})
		return warnings + zipWarnings, err
	}

	reporter.SetTotal(sumArchiveVolumeSizes(volumes))
	zipWarnings, err := r.forEachArchiveVolume(ctx, volumes, ar, info, func(ref locator.Ref, reader io.ReadCloser, readerInfo archiveReaderInfo) (int, error) {
		innerWarnings, err := r.withZipReader(ctx, ref, reader, readerInfo, nil, func(zr *zip.Reader) (int, error) {
			return r.listZipReader(ctx, zr, memberMatcher, reporter)
		})
		if err == nil && readerInfo.SizeKnown {
			reporter.AddDone(readerInfo.Size)
		}
		return innerWarnings, err
	})
	return warnings + zipWarnings, err
}

// listZipReader lists matching members from one zip reader.
func (r *Runner) listZipReader(ctx context.Context, zr *zip.Reader, memberMatcher *archivepath.CompiledPathMatcher, reporter *archiveprogress.Reporter) (int, error) {
	for _, zf := range zr.File {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		default:
		}
		if archivepath.ShouldSkipMemberWithMatcher(memberMatcher, zf.Name) {
			continue
		}
		reporter.ExternalLinef(r.stdout, "%s\n", zf.Name)
	}
	return 0, nil
}
