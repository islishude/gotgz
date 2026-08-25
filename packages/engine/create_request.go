package engine

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/islishude/gotgz/packages/archivepath"
	"github.com/islishude/gotgz/packages/cli"
	"github.com/islishude/gotgz/packages/locator"
)

// preparedCreateRequest contains validation that is safe to perform before a
// destination is opened and does not recursively walk local directories.
type preparedCreateRequest struct {
	opts             cli.Options
	excludeMatcher   *archivepath.CompiledPathMatcher
	outputPolicy     *createOutputPolicy
	tasks            []createPlanTask
	localTaskCount   int
	allIncludedLocal bool
}

func (r *Runner) prepareCreateRequest(ctx context.Context, opts cli.Options, excludeMatcher *archivepath.CompiledPathMatcher) (preparedCreateRequest, error) {
	outputPolicy, err := newCreateOutputPolicy(opts)
	if err != nil {
		return preparedCreateRequest{}, err
	}
	request := preparedCreateRequest{
		opts:             opts,
		excludeMatcher:   excludeMatcher,
		outputPolicy:     outputPolicy,
		tasks:            make([]createPlanTask, 0, len(opts.Members)),
		allIncludedLocal: true,
	}

	for index, member := range opts.Members {
		select {
		case <-ctx.Done():
			return preparedCreateRequest{}, ctx.Err()
		default:
		}

		ref, err := locator.ParseMember(member)
		if err != nil {
			return preparedCreateRequest{}, err
		}
		if err := outputPolicy.rejectExplicitMember(ref, member, opts.Chdir); err != nil {
			return preparedCreateRequest{}, err
		}

		switch ref.Kind {
		case locator.KindS3:
			if archivepath.MatchExcludeWithMatcher(excludeMatcher, ref.Key) {
				continue
			}
			request.allIncludedLocal = false
		case locator.KindLocal:
			if err := validateTopLevelLocalCreateMember(member, opts.Chdir); err != nil {
				return preparedCreateRequest{}, err
			}
			request.localTaskCount++
		default:
			return preparedCreateRequest{}, fmt.Errorf("unsupported member reference %q", member)
		}

		request.tasks = append(request.tasks, createPlanTask{
			index:  index,
			member: member,
			ref:    ref,
		})
	}
	return request, nil
}

func validateTopLevelLocalCreateMember(member, chdir string) error {
	path := member
	if chdir != "" {
		path = filepath.Join(chdir, member)
	}
	_, err := os.Lstat(filepath.Clean(path))
	return err
}

type createStrategy int

const (
	createStrategyFullPlan createStrategy = iota
	createStrategyStreamingPlan
)

func (r *Runner) selectCreateStrategy(request preparedCreateRequest, archiveRef locator.Ref) createStrategy {
	if request.opts.SplitSizeBytes > 0 || !request.allIncludedLocal || archiveRef.Kind != locator.KindLocal {
		return createStrategyFullPlan
	}
	capabilities := r.createWriterCapabilities(archiveRef)
	if !capabilities.rollbackSafe || !capabilities.singleLogicalOutput || !capabilities.exposesTempPaths {
		return createStrategyFullPlan
	}
	return createStrategyStreamingPlan
}
