package engine

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"sync"

	"github.com/islishude/gotgz/packages/archivepath"
	"github.com/islishude/gotgz/packages/cli"
	"github.com/islishude/gotgz/packages/locator"
)

const maxBuildCreatePlanConcurrency = 8

// createPlan captures one pre-scanned create workload so local filesystem
// walks can be reused by the later tar/zip write phase.
type createPlan struct {
	totalBytes    int64
	totalKnown    bool
	members       []createPlanMember
	outputSkipped bool
	spoolDir      string
	closeOnce     sync.Once
	closeErr      error
}

// createPlanMember stores one parsed create input and any pre-scanned local
// records associated with it.
type createPlanMember struct {
	ref              locator.Ref
	localPlanPath    string
	localRecordCount int64
}

// createPlanTask stores one parsed create member ready for concurrent work.
type createPlanTask struct {
	index  int
	member string
	ref    locator.Ref
}

// createPlanTaskResult stores one completed concurrent create-plan task.
type createPlanTaskResult struct {
	index      int
	member     createPlanMember
	totalBytes int64
	include    bool
}

// buildCreatePlan parses create members once, caches local walk results, and
// computes progress totals when possible.
func (r *Runner) buildCreatePlan(ctx context.Context, opts cli.Options) (_ *createPlan, retErr error) {
	request, err := r.prepareCreateRequest(ctx, opts, nil)
	if err != nil {
		return nil, err
	}
	return r.buildPreparedCreatePlan(ctx, request)
}

func (r *Runner) buildPreparedCreatePlan(ctx context.Context, request preparedCreateRequest) (_ *createPlan, retErr error) {
	plan := &createPlan{
		totalKnown: true,
		members:    make([]createPlanMember, 0, len(request.opts.Members)),
	}
	defer func() {
		if retErr != nil {
			retErr = errors.Join(retErr, plan.Close())
		}
	}()
	workerCount := buildCreatePlanWorkerCount(len(request.tasks))
	if workerCount == 0 {
		return plan, nil
	}
	if request.localTaskCount > 0 {
		spoolDir, err := os.MkdirTemp("", "gotgz-create-plan-*")
		if err != nil {
			return nil, fmt.Errorf("create plan spool directory: %w", err)
		}
		plan.spoolDir = spoolDir
		if err := os.Chmod(plan.spoolDir, 0o700); err != nil {
			return nil, fmt.Errorf("secure plan spool directory: %w", err)
		}
	}

	workCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	metadataLimiter := newCreatePlanMetadataLimiter(defaultCreatePlanMetadataConcurrency())

	tasksCh := make(chan createPlanTask)
	resultsCh := make(chan createPlanTaskResult, workerCount)
	orderedResults := make([]createPlanTaskResult, len(request.opts.Members))

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

					result, include, err := r.runCreatePlanTask(workCtx, task, request.opts.Chdir, plan.spoolDir, request.excludeMatcher, request.outputPolicy, metadataLimiter)
					if err != nil {
						cancel(err)
						return
					}
					result.index = task.index
					result.include = include

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
		for _, task := range request.tasks {
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
		orderedResults[result.index] = result
	}

	if err := context.Cause(workCtx); err != nil {
		return nil, err
	}

	for _, result := range orderedResults {
		if !result.include {
			continue
		}
		plan.members = append(plan.members, result.member)
		plan.totalBytes = addCreatePlanSize(plan.totalBytes, result.totalBytes)
	}
	plan.outputSkipped = request.outputPolicy.outputWasSkipped()

	return plan, nil
}

// Close removes all private local plan files. It is idempotent so callers can
// use explicit pre-commit cleanup together with deferred failure cleanup.
func (p *createPlan) Close() error {
	if p == nil {
		return nil
	}
	p.closeOnce.Do(func() {
		if p.spoolDir == "" {
			return
		}
		if err := os.RemoveAll(p.spoolDir); err != nil {
			p.closeErr = fmt.Errorf("remove create plan spool directory %q: %w", p.spoolDir, err)
		}
	})
	return p.closeErr
}

// addCreatePlanSize accumulates progress totals without wrapping int64.
func addCreatePlanSize(total, size int64) int64 {
	if size <= 0 || total == math.MaxInt64 {
		return total
	}
	if math.MaxInt64-total < size {
		return math.MaxInt64
	}
	return total + size
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
func (r *Runner) runCreatePlanTask(ctx context.Context, task createPlanTask, chdir, spoolDir string, excludeMatcher *archivepath.CompiledPathMatcher, outputPolicy *createOutputPolicy, metadataLimiter *createPlanMetadataLimiter) (createPlanTaskResult, bool, error) {
	switch task.ref.Kind {
	case locator.KindS3:
		meta, err := r.storage.statS3Object(ctx, task.ref)
		if err != nil {
			return createPlanTaskResult{}, false, err
		}
		return createPlanTaskResult{
			index:      task.index,
			member:     createPlanMember{ref: task.ref},
			totalBytes: meta.Size,
		}, true, nil
	case locator.KindLocal:
		planPath, size, count, err := spoolLocalCreateRecordsWithLimiter(ctx, spoolDir, task.member, chdir, excludeMatcher, outputPolicy, metadataLimiter)
		if err != nil {
			return createPlanTaskResult{}, false, err
		}
		if count == 0 {
			return createPlanTaskResult{}, false, nil
		}
		return createPlanTaskResult{
			index: task.index,
			member: createPlanMember{
				ref:              task.ref,
				localPlanPath:    planPath,
				localRecordCount: count,
			},
			totalBytes: size,
		}, true, nil
	default:
		return createPlanTaskResult{}, false, fmt.Errorf("unsupported member reference %q", task.member)
	}
}
