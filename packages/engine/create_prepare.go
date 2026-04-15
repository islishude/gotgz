package engine

import (
	"context"

	"github.com/islishude/gotgz/packages/archivepath"
	"github.com/islishude/gotgz/packages/archiveprogress"
	"github.com/islishude/gotgz/packages/cli"
	"github.com/islishude/gotgz/packages/locator"
)

// preparedCreateInput captures the normalized archive target together with the
// create input source shared by tar and zip writers.
type preparedCreateInput struct {
	archiveRef locator.Ref
	source     createInputSource
}

// prepareCreateInput resolves create-mode archive settings, loads excludes, and
// prepares the input source before format-specific writing begins.
func (r *Runner) prepareCreateInput(ctx context.Context, opts cli.Options, archiveRef locator.Ref, reporter *archiveprogress.Reporter) (preparedCreateInput, error) {
	resolvedArchiveRef, err := archiveRef.WithArchiveSuffix(opts.Suffix)
	if err != nil {
		return preparedCreateInput{}, err
	}

	excludes, err := archivepath.LoadExcludePatterns(opts.Exclude, opts.ExcludeFrom)
	if err != nil {
		return preparedCreateInput{}, err
	}
	excludeMatcher := archivepath.NewCompiledPathMatcher(excludes)

	source, err := r.newCreateInputSource(ctx, opts, excludeMatcher, reporter.Enabled())
	if err != nil {
		return preparedCreateInput{}, err
	}
	reporter.SetTotal(source.Total())

	return preparedCreateInput{
		archiveRef: resolvedArchiveRef,
		source:     source,
	}, nil
}
