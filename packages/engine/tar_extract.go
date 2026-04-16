package engine

import (
	"archive/tar"
	"context"
	"fmt"
	"io"

	"github.com/islishude/gotgz/packages/archivepath"
	"github.com/islishude/gotgz/packages/archiveprogress"
	"github.com/islishude/gotgz/packages/archiveutil"
	"github.com/islishude/gotgz/packages/cli"
	"github.com/islishude/gotgz/packages/locator"
)

// runExtractTar extracts archive members from a tar input stream or split-volume set.
func (r *Runner) runExtractTar(ctx context.Context, opts cli.Options, reporter *archiveprogress.Reporter, ref locator.Ref, ar io.ReadCloser, info archiveReaderInfo) (int, error) {
	volumes, err := r.resolveArchiveVolumes(ctx, ref, info)
	if err != nil {
		return 0, err
	}

	scan := func(scanReader io.ReadCloser, scanInfo archiveReaderInfo) (int, error) {
		return r.runExtractTarReader(ctx, opts, reporter, scanReader, scanInfo)
	}

	if len(volumes) == 1 {
		reporter.SetTotal(info.Size, info.SizeKnown)
		return scan(ar, info)
	}
	if !shouldPlanSplitExtract(opts, volumes) {
		return r.scanTarArchiveFromVolumes(ctx, opts, reporter, volumes, ar, scan)
	}

	target, err := locator.ParseExtractTarget(opts.Chdir, opts.S3CacheControl, opts.S3ObjectTags)
	if err != nil {
		return 0, err
	}

	plan, err := r.planSplitTarExtract(ctx, opts, volumes, ar, info, target)
	if err != nil {
		return 0, err
	}
	if !plan.parallel {
		return r.runSplitTarExtractSequential(ctx, opts, reporter, volumes)
	}
	return r.runSplitTarExtractParallel(ctx, opts, reporter, volumes)
}

// runExtractTarReader extracts archive members from a single tar volume reader.
func (r *Runner) runExtractTarReader(ctx context.Context, opts cli.Options, reporter *archiveprogress.Reporter, ar io.ReadCloser, info archiveReaderInfo) (int, error) {
	policy := opts.ResolvePermissionPolicy()
	metadataPolicy := opts.ResolveMetadataPolicy()
	memberMatcher := archivepath.NewMemberMatcher(opts.Members, opts.Wildcards)

	if opts.ToStdout {
		return r.scanTarArchiveFromReader(ctx, opts, reporter, info, opts.Archive, ar, func(hdr *tar.Header, tr *tar.Reader) (int, error) {
			if archivepath.ShouldSkipMemberWithMatcher(memberMatcher, hdr.Name) {
				if _, err := archiveutil.CopyWithContext(ctx, io.Discard, tr); err != nil {
					return 0, err
				}
				return 0, nil
			}
			if _, ok := archivepath.StripPathComponents(hdr.Name, opts.StripComponents); !ok {
				if _, err := archiveutil.CopyWithContext(ctx, io.Discard, io.LimitReader(tr, hdr.Size)); err != nil {
					return 0, err
				}
				return 0, nil
			}
			if hdr.Typeflag != tar.TypeReg {
				if _, err := archiveutil.CopyWithContext(ctx, io.Discard, tr); err != nil {
					return 0, err
				}
				return 0, nil
			}
			_, err := archiveutil.CopyWithContext(ctx, r.stdout, tr)
			return 0, err
		})
	}

	parsedTarget, err := locator.ParseExtractTarget(opts.Chdir, opts.S3CacheControl, opts.S3ObjectTags)
	if err != nil {
		return 0, err
	}
	target := opts.Chdir
	if target == "" {
		target = "."
	}
	var safetyCache *archivepath.PathSafetyCache
	if parsedTarget.Kind == locator.KindLocal || parsedTarget.Kind == locator.KindStdio {
		safetyCache = archivepath.NewPathSafetyCache()
	}

	return r.scanTarArchiveFromReader(ctx, opts, reporter, info, opts.Archive, ar, func(hdr *tar.Header, tr *tar.Reader) (int, error) {
		if archivepath.ShouldSkipMemberWithMatcher(memberMatcher, hdr.Name) {
			if _, err := archiveutil.CopyWithContext(ctx, io.Discard, tr); err != nil {
				return 0, err
			}
			return 0, nil
		}
		extractName, ok := archivepath.StripPathComponents(hdr.Name, opts.StripComponents)
		if !ok {
			if _, err := archiveutil.CopyWithContext(ctx, io.Discard, io.LimitReader(tr, hdr.Size)); err != nil {
				return 0, err
			}
			return 0, nil
		}
		effectiveHdr := *hdr
		effectiveHdr.Name = extractName
		if opts.Verbose {
			reporter.BeforeExternalLineOutput()
			_, _ = fmt.Fprintln(r.stdout, effectiveHdr.Name)
			reporter.AfterExternalLineOutput()
		}
		return r.dispatchExtractTarget(
			parsedTarget,
			target,
			func(target locator.Ref) (int, error) {
				return r.extractToS3(ctx, target, &effectiveHdr, tr, reporter)
			},
			func(base string) (int, error) {
				return r.extractToLocal(ctx, base, &effectiveHdr, tr, policy, metadataPolicy, safetyCache, reporter)
			},
		)
	})
}

// runSplitTarExtractSequential replays split tar extraction serially after planning consumed the original readers.
func (r *Runner) runSplitTarExtractSequential(ctx context.Context, opts cli.Options, reporter *archiveprogress.Reporter, volumes []archiveVolume) (int, error) {
	scan := func(scanReader io.ReadCloser, scanInfo archiveReaderInfo) (int, error) {
		return r.runExtractTarReader(ctx, opts, reporter, scanReader, scanInfo)
	}
	return r.scanTarArchiveFromVolumes(ctx, opts, reporter, volumes, nil, scan)
}

// runSplitTarExtractParallel reopens split tar volumes and extracts them with one worker per volume task.
func (r *Runner) runSplitTarExtractParallel(ctx context.Context, opts cli.Options, reporter *archiveprogress.Reporter, volumes []archiveVolume) (int, error) {
	reporter.SetTotal(sumArchiveVolumeSizes(volumes))
	return r.executeSplitExtractVolumes(ctx, volumes, func(ctx context.Context, _ int, volume archiveVolume) (int, error) {
		reader, runtimeInfo, err := r.openArchiveReader(ctx, volume.ref)
		if err != nil {
			return 0, err
		}
		defer reader.Close() //nolint:errcheck

		info := mergeArchiveReaderInfo(volume.info, runtimeInfo)
		return r.runExtractTarReader(ctx, opts, reporter, reader, info)
	})
}
