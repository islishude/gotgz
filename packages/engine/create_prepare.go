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
	warnings   int
}

// prepareCreateInput resolves create-mode archive settings, loads excludes, and
// prepares the input source before format-specific writing begins.
func (r *Runner) prepareCreateInput(ctx context.Context, opts cli.Options, archiveRef locator.Ref, reporter *archiveprogress.Reporter) (preparedCreateInput, error) {
	if opts.Wildcards {
		if err := archivepath.ValidateGlobPatterns(opts.Members); err != nil {
			return preparedCreateInput{}, err
		}
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
	warnings := 0
	if planned, ok := source.(plannedCreateInputSource); ok && planned.plan.outputSkipped {
		warnings += r.warnf(reporter, "create: archive output inside an input tree was skipped")
	}

	return preparedCreateInput{
		archiveRef: archiveRef,
		source:     source,
		warnings:   warnings,
	}, nil
}
