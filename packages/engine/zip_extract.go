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
			zipWarnings, err := r.runExtractZipStdoutVolume(ctx, opts, reporter, archiveRef, ar, info, memberMatcher, true)
			return warnings + zipWarnings, err
		}
		zipWarnings, err := r.runSplitZipStdoutExtractSequential(ctx, opts, reporter, volumes, ar, info, memberMatcher)
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
		zipWarnings, err := r.runExtractZipTargetVolume(ctx, opts, reporter, archiveRef, ar, info, parsedTarget, target, policy, safetyCache, memberMatcher, true)
		return warnings + zipWarnings, err
	}

	if !shouldPlanSplitExtract(opts, volumes) {
		zipWarnings, err := r.runSplitZipExtractSequential(ctx, opts, reporter, volumes, ar, info, parsedTarget, target, policy, safetyCache, memberMatcher)
		return warnings + zipWarnings, err
	}

	planningTotal, planningKnown := sumArchiveVolumeSizes(volumes)
	reporter.ResetProgress(planningTotal, planningKnown)

	plan, err := r.planSplitZipExtract(ctx, opts, reporter, volumes, ar, info, parsedTarget)
	if err != nil {
		return warnings, err
	}
	reporter.ResetProgress(plan.zipPayloadBytes, true)
	if !plan.parallel {
		zipWarnings, err := r.runSplitZipExtractSequential(ctx, opts, reporter, volumes, nil, info, parsedTarget, target, policy, safetyCache, memberMatcher)
		return warnings + zipWarnings, err
	}

	zipWarnings, err := r.runSplitZipExtractParallel(ctx, opts, reporter, volumes, parsedTarget, target, policy, safetyCache, memberMatcher)
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
			r.writeOutputLineLocked(r.stdout, reporter, "%s\n", extractName)
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

// runExtractZipStdoutVolume extracts one zip reader to stdout and optionally sets the payload total.
func (r *Runner) runExtractZipStdoutVolume(ctx context.Context, opts cli.Options, reporter *archiveprogress.Reporter, archiveRef locator.Ref, ar io.ReadCloser, info archiveReaderInfo, memberMatcher *archivepath.CompiledPathMatcher, setTotal bool) (int, error) {
	return r.withZipReader(ctx, archiveRef, ar, info, nil, func(zr *zip.Reader) (int, error) {
		if setTotal {
			reporter.SetTotal(matchingZipStdoutPayloadBytes(zr, memberMatcher, opts), true)
		}
		return r.extractZipToStdout(ctx, zr, memberMatcher, opts, reporter)
	})
}

// runExtractZipTargetVolume extracts one zip reader into the resolved target and optionally sets the payload total.
func (r *Runner) runExtractZipTargetVolume(ctx context.Context, opts cli.Options, reporter *archiveprogress.Reporter, archiveRef locator.Ref, ar io.ReadCloser, info archiveReaderInfo, parsedTarget locator.Ref, target string, policy PermissionPolicy, safetyCache *archivepath.PathSafetyCache, memberMatcher *archivepath.CompiledPathMatcher, setTotal bool) (int, error) {
	return r.withZipReader(ctx, archiveRef, ar, info, nil, func(zr *zip.Reader) (int, error) {
		if setTotal {
			reporter.SetTotal(matchingZipExtractPayloadBytes(zr, memberMatcher, opts.StripComponents), true)
		}
		return r.extractZipEntries(ctx, zr, opts, reporter, parsedTarget, target, policy, safetyCache, memberMatcher)
	})
}

// runSplitZipStdoutExtractSequential replays split zip stdout extraction without planner pre-scans.
func (r *Runner) runSplitZipStdoutExtractSequential(ctx context.Context, opts cli.Options, reporter *archiveprogress.Reporter, volumes []archiveVolume, first io.ReadCloser, firstInfo archiveReaderInfo, memberMatcher *archivepath.CompiledPathMatcher) (int, error) {
	return r.forEachArchiveVolume(ctx, volumes, first, firstInfo, func(ref locator.Ref, reader io.ReadCloser, readerInfo archiveReaderInfo) (int, error) {
		return r.runExtractZipStdoutVolume(ctx, opts, reporter, ref, reader, readerInfo, memberMatcher, false)
	})
}

// runSplitZipExtractSequential replays split zip target extraction serially after planner consumption.
func (r *Runner) runSplitZipExtractSequential(ctx context.Context, opts cli.Options, reporter *archiveprogress.Reporter, volumes []archiveVolume, first io.ReadCloser, firstInfo archiveReaderInfo, parsedTarget locator.Ref, target string, policy PermissionPolicy, safetyCache *archivepath.PathSafetyCache, memberMatcher *archivepath.CompiledPathMatcher) (int, error) {
	return r.forEachArchiveVolume(ctx, volumes, first, firstInfo, func(ref locator.Ref, reader io.ReadCloser, readerInfo archiveReaderInfo) (int, error) {
		return r.runExtractZipTargetVolume(ctx, opts, reporter, ref, reader, readerInfo, parsedTarget, target, policy, safetyCache, memberMatcher, false)
	})
}

// runSplitZipExtractParallel reopens split zip volumes and extracts them through the shared worker pool.
func (r *Runner) runSplitZipExtractParallel(ctx context.Context, opts cli.Options, reporter *archiveprogress.Reporter, volumes []archiveVolume, parsedTarget locator.Ref, target string, policy PermissionPolicy, safetyCache *archivepath.PathSafetyCache, memberMatcher *archivepath.CompiledPathMatcher) (int, error) {
	return r.executeSplitExtractVolumes(ctx, volumes, func(ctx context.Context, _ int, volume archiveVolume) (int, error) {
		reader, runtimeInfo, err := r.openArchiveReader(ctx, volume.ref)
		if err != nil {
			return 0, err
		}
		defer reader.Close() //nolint:errcheck

		info := mergeArchiveReaderInfo(volume.info, runtimeInfo)
		return r.runExtractZipTargetVolume(ctx, opts, reporter, volume.ref, reader, info, parsedTarget, target, policy, safetyCache, memberMatcher, false)
	})
}
