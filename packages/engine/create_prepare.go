package engine

import (
	"context"
	"fmt"

	"github.com/islishude/gotgz/packages/archivepath"
	"github.com/islishude/gotgz/packages/archiveprogress"
	"github.com/islishude/gotgz/packages/cli"
	"github.com/islishude/gotgz/packages/locator"
)

// preparedCreateInput captures the normalized archive target together with the
// create input source shared by tar and zip writers.
type preparedCreateInput struct {
	archiveRef   locator.Ref
	source       createInputSource
	strategy     createStrategy
	outputPolicy *createOutputPolicy
	warnings     int
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

	request, err := r.prepareCreateRequest(ctx, opts, excludeMatcher)
	if err != nil {
		return preparedCreateInput{}, err
	}
	strategy := r.selectCreateStrategy(request, archiveRef)
	var source createInputSource
	switch strategy {
	case createStrategyStreamingPlan:
		source, err = newStreamingCreateInputSource(request, reporter)
	case createStrategyFullPlan:
		var plan *createPlan
		plan, err = r.buildPreparedCreatePlan(ctx, request)
		if err == nil {
			source = plannedCreateInputSource{plan: plan}
		}
	default:
		err = fmt.Errorf("unsupported create strategy %d", strategy)
	}
	if err != nil {
		return preparedCreateInput{}, err
	}
	reporter.SetTotal(source.Total())
	warnings := 0
	if planned, ok := source.(plannedCreateInputSource); ok && planned.plan.outputSkipped {
		warnings += r.warnf(reporter, "create: archive output inside an input tree was skipped")
	}

	return preparedCreateInput{
		archiveRef:   archiveRef,
		source:       source,
		strategy:     strategy,
		outputPolicy: request.outputPolicy,
		warnings:     warnings,
	}, nil
}

func (p preparedCreateInput) registerWriterArtifacts(writer any) error {
	if p.strategy != createStrategyStreamingPlan {
		return nil
	}
	artifacts, ok := writer.(archiveWriteArtifacts)
	if !ok {
		return fmt.Errorf("streaming archive writer does not expose temporary output paths")
	}
	return p.outputPolicy.registerEphemeralLocalPaths(artifacts.EphemeralLocalPaths())
}

func (p preparedCreateInput) streamingOutputWasSkipped() bool {
	return p.strategy == createStrategyStreamingPlan && p.outputPolicy.outputWasSkipped()
}
