package engine

import (
	"archive/zip"
	"compress/flate"
	"context"
	"fmt"
	"io"
	"time"

	"github.com/islishude/gotgz/packages/cli"
	"github.com/islishude/gotgz/packages/locator"
)

// runCreateZip writes create-mode output in zip format.
func (r *Runner) runCreateZip(ctx context.Context, opts cli.Options, archiveRef locator.Ref) (warnings int, retErr error) {
	reporter := newProgressReporter(r.stderr, opts.Progress, 0, false, time.Now(), opts.Verbose)
	defer reporter.Finish()

	archiveRef, err := applyArchiveSuffix(archiveRef, opts.Suffix)
	if err != nil {
		return 0, err
	}

	warnings += r.warnZipCreateOptions(opts, reporter)

	aw, err := r.openArchiveWriter(ctx, archiveRef)
	if err != nil {
		return warnings, err
	}
	defer func() {
		if cerr := aw.Close(); cerr != nil && retErr == nil {
			retErr = fmt.Errorf("closing archive: %w", cerr)
		}
	}()

	zw := zip.NewWriter(aw)
	defer func() {
		if cerr := zw.Close(); cerr != nil && retErr == nil {
			retErr = fmt.Errorf("closing zip writer: %w", cerr)
		}
	}()

	if opts.CompressionLevel != nil {
		level := *opts.CompressionLevel
		zw.RegisterCompressor(zip.Deflate, func(dst io.Writer) (io.WriteCloser, error) {
			return flate.NewWriter(dst, level)
		})
	}

	excludes, err := loadExcludePatterns(opts.Exclude, opts.ExcludeFrom)
	if err != nil {
		return warnings, err
	}
	excludeMatcher := newCompiledPathMatcher(excludes)
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
func (r *Runner) runListZip(ctx context.Context, opts cli.Options, reporter *progressReporter, archiveRef locator.Ref, ar io.ReadCloser, info archiveReaderInfo) (int, error) {
	warnings := r.warnZipReadOptions(opts, reporter)
	reporter.SetTotal(info.Size, info.SizeKnown)
	memberMatcher := newMemberMatcher(opts)
	zipWarnings, err := r.withZipReader(ctx, archiveRef, ar, info, reporter, func(zr *zip.Reader) (int, error) {
		innerWarnings := 0
		for _, zf := range zr.File {
			select {
			case <-ctx.Done():
				return innerWarnings, ctx.Err()
			default:
			}
			if shouldSkipMemberWithMatcher(memberMatcher, zf.Name) {
				continue
			}
			reporter.beforeExternalLineOutput()
			_, _ = fmt.Fprintln(r.stdout, zf.Name)
			reporter.afterExternalLineOutput()
		}
		return innerWarnings, nil
	})
	return warnings + zipWarnings, err
}

// runExtractZip extracts archive members from a zip input stream.
func (r *Runner) runExtractZip(ctx context.Context, opts cli.Options, reporter *progressReporter, archiveRef locator.Ref, ar io.ReadCloser, info archiveReaderInfo) (int, error) {
	policy := resolvePolicy(opts)
	warnings := r.warnZipReadOptions(opts, reporter)

	if opts.ToStdout {
		memberMatcher := newMemberMatcher(opts)
		zipWarnings, err := r.withZipReader(ctx, archiveRef, ar, info, nil, func(zr *zip.Reader) (int, error) {
			total := totalZipPayloadBytes(zr, func(zf *zip.File) bool {
				if shouldSkipMemberWithMatcher(memberMatcher, zf.Name) {
					return false
				}
				name, ok := stripPathComponents(zf.Name, opts.StripComponents)
				if !ok {
					return false
				}
				return name != "" && isZipRegular(zf)
			})
			reporter.SetTotal(total, true)
			return r.extractZipToStdout(ctx, zr, memberMatcher, opts, reporter)
		})
		return warnings + zipWarnings, err
	}

	parsedTarget, err := parseExtractTarget(opts.Chdir, opts.S3CacheControl, opts.S3ObjectTags)
	if err != nil {
		return warnings, err
	}
	target := opts.Chdir
	if target == "" {
		target = "."
	}
	var safetyCache *pathSafetyCache
	if parsedTarget.Kind == locator.KindLocal || parsedTarget.Kind == locator.KindStdio {
		safetyCache = newPathSafetyCache()
	}
	memberMatcher := newMemberMatcher(opts)

	zipWarnings, err := r.withZipReader(ctx, archiveRef, ar, info, nil, func(zr *zip.Reader) (int, error) {
		total := totalZipPayloadBytes(zr, func(zf *zip.File) bool {
			if shouldSkipMemberWithMatcher(memberMatcher, zf.Name) {
				return false
			}
			name, ok := stripPathComponents(zf.Name, opts.StripComponents)
			return ok && name != ""
		})
		reporter.SetTotal(total, true)

		innerWarnings := 0
		for _, zf := range zr.File {
			select {
			case <-ctx.Done():
				return innerWarnings, ctx.Err()
			default:
			}
			if shouldSkipMemberWithMatcher(memberMatcher, zf.Name) {
				continue
			}
			extractName, ok := stripPathComponents(zf.Name, opts.StripComponents)
			if !ok || extractName == "" {
				continue
			}
			if opts.Verbose {
				reporter.beforeExternalLineOutput()
				_, _ = fmt.Fprintln(r.stdout, extractName)
				reporter.afterExternalLineOutput()
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
			innerWarnings += w
			if err != nil {
				return innerWarnings, err
			}
		}
		return innerWarnings, nil
	})
	return warnings + zipWarnings, err
}
