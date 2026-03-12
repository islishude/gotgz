package engine

import (
	"archive/zip"
	"context"
	"fmt"
	"io"
	"math"

	"github.com/islishude/gotgz/packages/archivepath"
	"github.com/islishude/gotgz/packages/archiveprogress"
	"github.com/islishude/gotgz/packages/cli"
	"github.com/islishude/gotgz/packages/locator"
)

// runCreateZip writes create-mode output in zip format.
func (r *Runner) runCreateZip(ctx context.Context, opts cli.Options, archiveRef locator.Ref, reporter *archiveprogress.Reporter) (warnings int, retErr error) {
	archiveRef, err := archiveRef.WithArchiveSuffix(opts.Suffix)
	if err != nil {
		return 0, err
	}

	warnings += r.warnZipCreateOptions(opts, reporter)

	zw, err := r.newZipArchiveWriter(ctx, opts, archiveRef)
	if err != nil {
		return warnings, err
	}
	defer func() {
		if cerr := zw.Close(); cerr != nil && retErr == nil {
			retErr = cerr
		}
	}()

	excludes, err := archivepath.LoadExcludePatterns(opts.Exclude, opts.ExcludeFrom)
	if err != nil {
		return warnings, err
	}
	excludeMatcher := archivepath.NewCompiledPathMatcher(excludes)
	plan, err := r.buildCreatePlanIfEnabled(ctx, opts, excludeMatcher, reporter)
	if err != nil {
		return warnings, err
	}
	if plan != nil {
		createWarnings, err := r.processCreatePlan(
			ctx,
			plan,
			func(ref locator.Ref) error {
				return r.addS3MemberZip(ctx, zw, ref, opts.Verbose, reporter)
			},
			func(entries []localCreateEntry) (int, error) {
				return r.addLocalEntriesZip(ctx, zw, entries, opts.Verbose, reporter)
			},
		)
		return warnings + createWarnings, err
	}

	createWarnings, err := r.processCreateMembers(
		ctx,
		opts,
		excludeMatcher,
		func(ref locator.Ref) error {
			return r.addS3MemberZip(ctx, zw, ref, opts.Verbose, reporter)
		},
		func(member string) (int, error) {
			return r.addLocalPathZip(ctx, zw, member, opts.Chdir, excludeMatcher, opts.Verbose, reporter)
		},
	)
	return warnings + createWarnings, err
}

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
		reporter.BeforeExternalLineOutput()
		_, _ = fmt.Fprintln(r.stdout, zf.Name)
		reporter.AfterExternalLineOutput()
	}
	return 0, nil
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

// sumSplitZipPayloadBytes pre-scans split zip volumes to compute one extraction total.
func (r *Runner) sumSplitZipPayloadBytes(ctx context.Context, volumes []archiveVolume, first io.ReadCloser, firstInfo archiveReaderInfo, match func(zf *zip.File) bool) (int64, error) {
	var total int64
	_, err := r.forEachArchiveVolume(ctx, volumes, first, firstInfo, func(ref locator.Ref, reader io.ReadCloser, readerInfo archiveReaderInfo) (int, error) {
		_, err := r.withZipReader(ctx, ref, reader, readerInfo, nil, func(zr *zip.Reader) (int, error) {
			total = addZipPayloadBytes(total, totalZipPayloadBytes(zr, match))
			return 0, nil
		})
		return 0, err
	})
	return total, err
}

// matchingZipStdoutPayloadBytes sums uncompressed bytes that would be written to stdout.
func matchingZipStdoutPayloadBytes(zr *zip.Reader, memberMatcher *archivepath.CompiledPathMatcher, opts cli.Options) int64 {
	return totalZipPayloadBytes(zr, func(zf *zip.File) bool {
		return shouldIncludeZipStdoutEntry(zf, memberMatcher, opts.StripComponents)
	})
}

// matchingZipExtractPayloadBytes sums uncompressed bytes that would be extracted.
func matchingZipExtractPayloadBytes(zr *zip.Reader, memberMatcher *archivepath.CompiledPathMatcher, stripComponents int) int64 {
	return totalZipPayloadBytes(zr, func(zf *zip.File) bool {
		return shouldIncludeZipExtractEntry(zf, memberMatcher, stripComponents)
	})
}

// shouldIncludeZipStdoutEntry reports whether one zip entry would be extracted to stdout.
func shouldIncludeZipStdoutEntry(zf *zip.File, memberMatcher *archivepath.CompiledPathMatcher, stripComponents int) bool {
	if archivepath.ShouldSkipMemberWithMatcher(memberMatcher, zf.Name) {
		return false
	}
	name, ok := archivepath.StripPathComponents(zf.Name, stripComponents)
	if !ok || name == "" {
		return false
	}
	return isZipRegular(zf)
}

// shouldIncludeZipExtractEntry reports whether one zip entry would be extracted to a directory or S3 target.
func shouldIncludeZipExtractEntry(zf *zip.File, memberMatcher *archivepath.CompiledPathMatcher, stripComponents int) bool {
	if archivepath.ShouldSkipMemberWithMatcher(memberMatcher, zf.Name) {
		return false
	}
	name, ok := archivepath.StripPathComponents(zf.Name, stripComponents)
	return ok && name != ""
}

// addZipPayloadBytes accumulates payload totals while clamping on overflow.
func addZipPayloadBytes(total int64, add int64) int64 {
	return addArchiveVolumeSize(total, add)
}
