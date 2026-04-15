package engine

import (
	"context"
	"fmt"

	"github.com/islishude/gotgz/packages/archivepath"
	"github.com/islishude/gotgz/packages/cli"
	"github.com/islishude/gotgz/packages/locator"
)

// createPlan captures one pre-scanned create workload so local filesystem
// walks can be reused by the later tar/zip write phase.
type createPlan struct {
	totalBytes int64
	totalKnown bool
	members    []createPlanMember
}

// createPlanMember stores one parsed create input and any pre-scanned local
// records associated with it.
type createPlanMember struct {
	ref          locator.Ref
	localRecords []localCreateRecord
}

// buildCreatePlan parses create members once, caches local walk results, and
// computes progress totals when possible.
func (r *Runner) buildCreatePlan(ctx context.Context, opts cli.Options, excludeMatcher *archivepath.CompiledPathMatcher) (*createPlan, error) {
	plan := &createPlan{
		totalKnown: true,
		members:    make([]createPlanMember, 0, len(opts.Members)),
	}

	for _, member := range opts.Members {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		ref, err := locator.ParseMember(member)
		if err != nil {
			return nil, err
		}

		switch ref.Kind {
		case locator.KindS3:
			if archivepath.MatchExcludeWithMatcher(excludeMatcher, ref.Key) {
				continue
			}
			plan.members = append(plan.members, createPlanMember{ref: ref})
			if !plan.totalKnown {
				continue
			}
			meta, err := r.storage.statS3Object(ctx, ref)
			if err != nil {
				plan.totalKnown = false
				continue
			}
			plan.totalBytes += meta.Size
		case locator.KindLocal:
			records, size, err := collectLocalCreateRecords(ctx, member, opts.Chdir, excludeMatcher)
			if err != nil {
				return nil, err
			}
			if len(records) == 0 {
				continue
			}
			plan.members = append(plan.members, createPlanMember{
				ref:          ref,
				localRecords: records,
			})
			plan.totalBytes += size
		default:
			return nil, fmt.Errorf("unsupported member reference %q", member)
		}
	}
	return plan, nil
}
