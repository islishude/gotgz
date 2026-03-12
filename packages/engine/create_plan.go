package engine

import (
	"context"
	"fmt"

	"github.com/islishude/gotgz/packages/archivepath"
	"github.com/islishude/gotgz/packages/archiveprogress"
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
// entries associated with it.
type createPlanMember struct {
	ref          locator.Ref
	localEntries []localCreateEntry
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
			entries, size, err := r.collectLocalCreateEntries(ctx, member, opts.Chdir, excludeMatcher)
			if err != nil {
				return nil, err
			}
			if len(entries) == 0 {
				continue
			}
			plan.members = append(plan.members, createPlanMember{
				ref:          ref,
				localEntries: entries,
			})
			plan.totalBytes += size
		default:
			return nil, fmt.Errorf("unsupported member reference %q", member)
		}
	}
	return plan, nil
}

// buildCreatePlanIfEnabled creates a reusable plan only when progress output is
// active and total-byte estimation is therefore useful.
func (r *Runner) buildCreatePlanIfEnabled(ctx context.Context, opts cli.Options, excludeMatcher *archivepath.CompiledPathMatcher, reporter *archiveprogress.Reporter) (*createPlan, error) {
	if reporter == nil || !reporter.Enabled() {
		return nil, nil
	}
	plan, err := r.buildCreatePlan(ctx, opts, excludeMatcher)
	if err != nil {
		return nil, err
	}
	reporter.SetTotal(plan.totalBytes, plan.totalKnown)
	return plan, nil
}

// processCreatePlan dispatches one pre-scanned create plan to the format-
// specific writers without rescanning local filesystem trees.
func (r *Runner) processCreatePlan(ctx context.Context, plan *createPlan, handleS3 func(ref locator.Ref) error, handleLocal func(entries []localCreateEntry) (int, error)) (int, error) {
	warnings := 0
	for _, member := range plan.members {
		select {
		case <-ctx.Done():
			return warnings, ctx.Err()
		default:
		}

		switch member.ref.Kind {
		case locator.KindS3:
			if err := handleS3(member.ref); err != nil {
				return warnings, err
			}
		case locator.KindLocal:
			w, err := handleLocal(member.localEntries)
			warnings += w
			if err != nil {
				return warnings, err
			}
		}
	}
	return warnings, nil
}
