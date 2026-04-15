package engine

import (
	"context"

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
	source, err := r.newCreateInputSource(ctx, opts, excludeMatcher, reporter != nil && reporter.Enabled())
	if err != nil {
		return warnings, err
	}
	reporter.SetTotal(source.Total())

	createWarnings, err := source.Visit(
		ctx,
		func(ref locator.Ref) error {
			return r.addS3MemberZip(ctx, zw, ref, opts.Verbose, reporter)
		},
		func(source localCreateSource) (int, error) {
			return r.addLocalZipSource(ctx, zw, source, opts.Verbose, reporter)
		},
	)
	return warnings + createWarnings, err
}
