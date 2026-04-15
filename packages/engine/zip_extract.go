package engine

import (
	"archive/zip"
	"context"
	"fmt"
	"io"

	"github.com/islishude/gotgz/packages/archivepath"
	"github.com/islishude/gotgz/packages/archiveprogress"
	"github.com/islishude/gotgz/packages/cli"
	"github.com/islishude/gotgz/packages/locator"
)

// runExtractZip extracts archive members from a zip input stream.
func (r *Runner) runExtractZip(ctx context.Context, opts cli.Options, reporter *archiveprogress.Reporter, archiveRef locator.Ref, ar io.ReadCloser, info archiveReaderInfo) (int, error) {
	policy := opts.ResolvePermissionPolicy()
	warnings := r.warnZipReadOptions(opts, reporter)
	memberMatcher := archivepath.NewMemberMatcher(opts.Members, opts.Wildcards)
	volumes, err := r.resolveArchiveVolumes(ctx, archiveRef, info)
	if err != nil {
		return warnings, err
	}

	if opts.ToStdout {
		if len(volumes) == 1 {
			zipWarnings, err := r.withZipReader(ctx, archiveRef, ar, info, nil, func(zr *zip.Reader) (int, error) {
				reporter.SetTotal(matchingZipStdoutPayloadBytes(zr, memberMatcher, opts), true)
				return r.extractZipToStdout(ctx, zr, memberMatcher, opts, reporter)
			})
			return warnings + zipWarnings, err
		}

		firstReader := ar
		if reporter.Enabled() {
			total, err := r.sumSplitZipPayloadBytes(ctx, volumes, ar, info, func(zf *zip.File) bool {
				return shouldIncludeZipStdoutEntry(zf, memberMatcher, opts.StripComponents)
			})
			if err != nil {
				return warnings, err
			}
			reporter.SetTotal(total, true)
			firstReader = nil
		}
		zipWarnings, err := r.forEachArchiveVolume(ctx, volumes, firstReader, info, func(ref locator.Ref, reader io.ReadCloser, readerInfo archiveReaderInfo) (int, error) {
			return r.withZipReader(ctx, ref, reader, readerInfo, nil, func(zr *zip.Reader) (int, error) {
				return r.extractZipToStdout(ctx, zr, memberMatcher, opts, reporter)
			})
		})
		return warnings + zipWarnings, err
	}

	parsedTarget, err := locator.ParseExtractTarget(opts.Chdir, opts.S3CacheControl, opts.S3ObjectTags)
	if err != nil {
		return warnings, err
	}
	target := opts.Chdir
	if target == "" {
		target = "."
	}
	var safetyCache *archivepath.PathSafetyCache
	if parsedTarget.Kind == locator.KindLocal || parsedTarget.Kind == locator.KindStdio {
		safetyCache = archivepath.NewPathSafetyCache()
	}

	if len(volumes) == 1 {
		zipWarnings, err := r.withZipReader(ctx, archiveRef, ar, info, nil, func(zr *zip.Reader) (int, error) {
			reporter.SetTotal(matchingZipExtractPayloadBytes(zr, memberMatcher, opts.StripComponents), true)
			return r.extractZipEntries(ctx, zr, opts, reporter, parsedTarget, target, policy, safetyCache, memberMatcher)
		})
		return warnings + zipWarnings, err
	}

	firstReader := ar
	if reporter.Enabled() {
		total, err := r.sumSplitZipPayloadBytes(ctx, volumes, ar, info, func(zf *zip.File) bool {
			return shouldIncludeZipExtractEntry(zf, memberMatcher, opts.StripComponents)
		})
		if err != nil {
			return warnings, err
		}
		reporter.SetTotal(total, true)
		firstReader = nil
	}

	zipWarnings, err := r.forEachArchiveVolume(ctx, volumes, firstReader, info, func(ref locator.Ref, reader io.ReadCloser, readerInfo archiveReaderInfo) (int, error) {
		return r.withZipReader(ctx, ref, reader, readerInfo, nil, func(zr *zip.Reader) (int, error) {
			return r.extractZipEntries(ctx, zr, opts, reporter, parsedTarget, target, policy, safetyCache, memberMatcher)
		})
	})
	return warnings + zipWarnings, err
}

// extractZipEntries extracts matching members from one zip reader into the configured target.
func (r *Runner) extractZipEntries(ctx context.Context, zr *zip.Reader, opts cli.Options, reporter *archiveprogress.Reporter, parsedTarget locator.Ref, target string, policy PermissionPolicy, safetyCache *archivepath.PathSafetyCache, memberMatcher *archivepath.CompiledPathMatcher) (int, error) {
	warnings := 0
	for _, zf := range zr.File {
		select {
		case <-ctx.Done():
			return warnings, ctx.Err()
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

		w, err := r.dispatchExtractTarget(
			parsedTarget,
			target,
			func(target locator.Ref) (int, error) {
				return r.extractZipEntryToS3(ctx, target, zf, extractName, reporter)
			},
			func(base string) (int, error) {
				return r.extractZipEntryToLocal(ctx, base, zf, extractName, policy, safetyCache, reporter)
			},
		)
		warnings += w
		if err != nil {
			return warnings, err
		}
	}
	return warnings, nil
}
