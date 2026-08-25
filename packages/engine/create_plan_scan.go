package engine

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"runtime"
	"sync"

	"github.com/islishude/gotgz/packages/archivepath"
)

const maxCreatePlanMetadataConcurrency = 4

// createPlanRecord is the stable scanner-to-sink representation of one local
// archive member. It intentionally excludes FileInfo because replay refreshes
// filesystem metadata immediately before writing.
type createPlanRecord struct {
	Current     string
	ArchiveName string
	EntryType   fs.FileMode
}

type createPlanSink interface {
	Append(createPlanRecord) error
}

type createPlanMetadataLimiter struct {
	tokens chan struct{}
}

func newCreatePlanMetadataLimiter(concurrency int) *createPlanMetadataLimiter {
	if concurrency < 1 {
		concurrency = 1
	}
	return &createPlanMetadataLimiter{tokens: make(chan struct{}, concurrency)}
}

func defaultCreatePlanMetadataConcurrency() int {
	return min(max(2*runtime.GOMAXPROCS(0), 1), maxCreatePlanMetadataConcurrency)
}

func (l *createPlanMetadataLimiter) acquire(ctx context.Context) error {
	if l == nil {
		return nil
	}
	select {
	case l.tokens <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (l *createPlanMetadataLimiter) release() {
	if l != nil {
		<-l.tokens
	}
}

type createPlanMetadataOps struct {
	lstat    func(context.Context, string) (fs.FileInfo, error)
	readlink func(context.Context, string) (string, error)
}

func defaultCreatePlanMetadataOps() createPlanMetadataOps {
	return createPlanMetadataOps{
		lstat: func(_ context.Context, path string) (fs.FileInfo, error) {
			return os.Lstat(path)
		},
		readlink: func(_ context.Context, path string) (string, error) {
			return os.Readlink(path)
		},
	}
}

type createPlanScannerConfig struct {
	workerCount int
	windowSize  int
	limiter     *createPlanMetadataLimiter
	metadata    createPlanMetadataOps
}

func newCreatePlanScannerConfig(limiter *createPlanMetadataLimiter) createPlanScannerConfig {
	concurrency := defaultCreatePlanMetadataConcurrency()
	if limiter == nil {
		limiter = newCreatePlanMetadataLimiter(concurrency)
	} else {
		concurrency = cap(limiter.tokens)
	}
	return createPlanScannerConfig{
		workerCount: concurrency,
		windowSize:  4 * concurrency,
		limiter:     limiter,
		metadata:    defaultCreatePlanMetadataOps(),
	}
}

func (c createPlanScannerConfig) normalized() createPlanScannerConfig {
	if c.workerCount < 1 {
		c.workerCount = 1
	}
	if c.windowSize < 1 {
		c.windowSize = c.workerCount
	}
	if c.limiter == nil {
		c.limiter = newCreatePlanMetadataLimiter(c.workerCount)
	}
	defaults := defaultCreatePlanMetadataOps()
	if c.metadata.lstat == nil {
		c.metadata.lstat = defaults.lstat
	}
	if c.metadata.readlink == nil {
		c.metadata.readlink = defaults.readlink
	}
	return c
}

type createPlanMetadataJob struct {
	seq       uint64
	record    createPlanRecord
	entryType fs.FileMode
}

type createPlanMetadataResult struct {
	seq    uint64
	record createPlanRecord
	info   fs.FileInfo
	size   int64
	err    error
}

type createPlanProducerResult struct {
	scheduled uint64
	err       error
}

// scanLocalCreateRecords walks one member lexically while metadata workers
// inspect entries concurrently. Results are appended in walk order, and the
// outstanding sequence window bounds channels and the reorder map.
func scanLocalCreateRecords(
	ctx context.Context,
	member string,
	chdir string,
	excludeMatcher *archivepath.CompiledPathMatcher,
	outputPolicy *createOutputPolicy,
	spoolInfo fs.FileInfo,
	sink createPlanSink,
	config createPlanScannerConfig,
) (total int64, count int64, retErr error) {
	config = config.normalized()
	workCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	jobs := make(chan createPlanMetadataJob, config.workerCount)
	results := make(chan createPlanMetadataResult, config.workerCount)
	window := make(chan struct{}, config.windowSize)
	producerDone := make(chan createPlanProducerResult, 1)

	var workers sync.WaitGroup
	for range config.workerCount {
		workers.Go(func() {
			for job := range jobs {
				result := inspectCreatePlanMetadata(workCtx, job, config)
				select {
				case results <- result:
				case <-workCtx.Done():
					return
				}
			}
		})
	}

	go produceCreatePlanMetadataJobs(
		workCtx,
		member,
		chdir,
		excludeMatcher,
		outputPolicy,
		spoolInfo,
		config,
		window,
		jobs,
		producerDone,
	)
	go func() {
		workers.Wait()
		close(results)
	}()

	pending := make(map[uint64]createPlanMetadataResult, config.windowSize)
	var next uint64
	var pipelineErr error
	for result := range results {
		if pipelineErr != nil {
			continue
		}
		pending[result.seq] = result
		for {
			ordered, ok := pending[next]
			if !ok {
				break
			}
			delete(pending, next)
			<-window
			if ordered.err != nil {
				pipelineErr = ordered.err
				cancel(pipelineErr)
				break
			}
			if outputPolicy.shouldSkipEphemeralIdentity(ordered.info) {
				next++
				continue
			}
			if err := sink.Append(ordered.record); err != nil {
				pipelineErr = err
				cancel(pipelineErr)
				break
			}
			total = addCreatePlanSize(total, ordered.size)
			count++
			next++
		}
	}

	producer := <-producerDone
	if pipelineErr != nil {
		return total, count, pipelineErr
	}
	if producer.err != nil {
		return total, count, producer.err
	}
	if err := context.Cause(workCtx); err != nil {
		return total, count, err
	}
	if next != producer.scheduled || len(pending) != 0 {
		return total, count, fmt.Errorf("create plan metadata pipeline stopped after %d of %d records", next, producer.scheduled)
	}
	return total, count, nil
}

func produceCreatePlanMetadataJobs(
	ctx context.Context,
	member string,
	chdir string,
	excludeMatcher *archivepath.CompiledPathMatcher,
	outputPolicy *createOutputPolicy,
	spoolInfo fs.FileInfo,
	config createPlanScannerConfig,
	window chan struct{},
	jobs chan createPlanMetadataJob,
	done chan<- createPlanProducerResult,
) {
	defer close(jobs)
	var scheduled uint64
	err := walkLocalCreateMemberEntries(ctx, member, chdir, excludeMatcher, func(record localCreateRecord, entry fs.DirEntry) error {
		if entry.IsDir() {
			info, err := loadCreatePlanInfo(ctx, record.current, config)
			if err != nil {
				return err
			}
			if os.SameFile(spoolInfo, info) {
				return fs.SkipDir
			}
		}
		if outputPolicy.shouldSkipLocal(record.current) {
			return nil
		}

		select {
		case window <- struct{}{}:
		case <-ctx.Done():
			return context.Cause(ctx)
		}
		job := createPlanMetadataJob{
			seq: scheduled,
			record: createPlanRecord{
				Current:     record.current,
				ArchiveName: record.archiveName,
				EntryType:   entry.Type(),
			},
			entryType: entry.Type(),
		}
		select {
		case jobs <- job:
			scheduled++
			return nil
		case <-ctx.Done():
			<-window
			return context.Cause(ctx)
		}
	})
	done <- createPlanProducerResult{scheduled: scheduled, err: err}
}

func inspectCreatePlanMetadata(ctx context.Context, job createPlanMetadataJob, config createPlanScannerConfig) createPlanMetadataResult {
	result := createPlanMetadataResult{seq: job.seq, record: job.record}
	switch {
	case job.entryType&os.ModeSymlink != 0:
		linkTarget, err := readCreatePlanLink(ctx, job.record.Current, config)
		if err != nil {
			result.err = err
			return result
		}
		result.err = validateCreateSymlinkTarget(job.record.ArchiveName, linkTarget)
	case job.entryType.IsRegular():
		info, err := loadCreatePlanInfo(ctx, job.record.Current, config)
		if err != nil {
			result.err = err
			return result
		}
		if info.Mode().IsRegular() {
			result.info = info
			result.size = info.Size()
		}
	}
	return result
}

func loadCreatePlanInfo(ctx context.Context, path string, config createPlanScannerConfig) (fs.FileInfo, error) {
	if err := config.limiter.acquire(ctx); err != nil {
		return nil, err
	}
	defer config.limiter.release()
	return config.metadata.lstat(ctx, path)
}

func readCreatePlanLink(ctx context.Context, path string, config createPlanScannerConfig) (string, error) {
	if err := config.limiter.acquire(ctx); err != nil {
		return "", err
	}
	defer config.limiter.release()
	return config.metadata.readlink(ctx, path)
}
