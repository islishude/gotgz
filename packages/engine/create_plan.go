package engine

import (
	"context"
	"fmt"
	"sync"

	"github.com/islishude/gotgz/packages/archivepath"
	"github.com/islishude/gotgz/packages/cli"
	"github.com/islishude/gotgz/packages/locator"
)

const maxBuildCreatePlanConcurrency = 8

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

// createPlanTask stores one parsed create member ready for concurrent work.
type createPlanTask struct {
	member string
	ref    locator.Ref
}

// createPlanTaskResult stores one completed concurrent create-plan task.
type createPlanTaskResult struct {
	member     createPlanMember
	totalBytes int64
}

// buildCreatePlan parses create members once, caches local walk results, and
// computes progress totals when possible.
func (r *Runner) buildCreatePlan(ctx context.Context, opts cli.Options, excludeMatcher *archivepath.CompiledPathMatcher) (*createPlan, error) {
	plan := &createPlan{
		totalKnown: true,
		members:    make([]createPlanMember, 0, len(opts.Members)),
	}
	tasks := make([]createPlanTask, 0, len(opts.Members))

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
		case locator.KindLocal:
		default:
			return nil, fmt.Errorf("unsupported member reference %q", member)
		}

		tasks = append(tasks, createPlanTask{
			member: member,
			ref:    ref,
		})
	}

	workerCount := buildCreatePlanWorkerCount(len(tasks))
	if workerCount == 0 {
		return plan, nil
	}

	workCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	tasksCh := make(chan createPlanTask)
	resultsCh := make(chan createPlanTaskResult, workerCount)

	var workers sync.WaitGroup
	for range workerCount {
		workers.Go(func() {
			for {
				select {
				case <-workCtx.Done():
					return
				case task, ok := <-tasksCh:
					if !ok {
						return
					}

					result, include, err := r.runCreatePlanTask(workCtx, task, opts.Chdir, excludeMatcher)
					if err != nil {
						cancel(err)
						return
					}
					if !include {
						continue
					}

					select {
					case resultsCh <- result:
					case <-workCtx.Done():
						return
					}
				}
			}
		})
	}

	go func() {
		defer close(tasksCh)
		for _, task := range tasks {
			select {
			case <-workCtx.Done():
				return
			case tasksCh <- task:
			}
		}
	}()

	go func() {
		workers.Wait()
		close(resultsCh)
	}()

	for result := range resultsCh {
		plan.members = append(plan.members, result.member)
		plan.totalBytes += result.totalBytes
	}

	if err := context.Cause(workCtx); err != nil {
		return nil, err
	}

	return plan, nil
}

// buildCreatePlanWorkerCount bounds the number of concurrent create-plan tasks.
func buildCreatePlanWorkerCount(taskCount int) int {
	if taskCount <= 0 {
		return 0
	}
	if taskCount < maxBuildCreatePlanConcurrency {
		return taskCount
	}
	return maxBuildCreatePlanConcurrency
}

// runCreatePlanTask executes one pre-scanned create member workload.
func (r *Runner) runCreatePlanTask(ctx context.Context, task createPlanTask, chdir string, excludeMatcher *archivepath.CompiledPathMatcher) (createPlanTaskResult, bool, error) {
	switch task.ref.Kind {
	case locator.KindS3:
		meta, err := r.storage.statS3Object(ctx, task.ref)
		if err != nil {
			return createPlanTaskResult{}, false, err
		}
		return createPlanTaskResult{
			member:     createPlanMember{ref: task.ref},
			totalBytes: meta.Size,
		}, true, nil
	case locator.KindLocal:
		records, size, err := collectLocalCreateRecords(ctx, task.member, chdir, excludeMatcher)
		if err != nil {
			return createPlanTaskResult{}, false, err
		}
		if len(records) == 0 {
			return createPlanTaskResult{}, false, nil
		}
		return createPlanTaskResult{
			member: createPlanMember{
				ref:          task.ref,
				localRecords: records,
			},
			totalBytes: size,
		}, true, nil
	default:
		return createPlanTaskResult{}, false, fmt.Errorf("unsupported member reference %q", task.member)
	}
}
