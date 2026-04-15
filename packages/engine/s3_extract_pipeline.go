package engine

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/islishude/gotgz/packages/archiveutil"
)

// s3ExtractTask represents one upload job scheduled by the concurrent extract pipeline.
type s3ExtractTask func(ctx context.Context) error

// s3ExtractPipeline limits object-level upload concurrency and cancels peer work
// after the first failure.
type s3ExtractPipeline struct {
	parent context.Context
	ctx    context.Context
	cancel context.CancelFunc

	tasks chan s3ExtractTask

	closeOnce sync.Once
	wg        sync.WaitGroup

	errMu sync.Mutex
	err   error
}

// newS3ExtractPipeline creates one worker pool for archive->S3 extract tasks.
func newS3ExtractPipeline(parent context.Context, workers int) *s3ExtractPipeline {
	if workers < 1 {
		workers = 1
	}
	ctx, cancel := context.WithCancel(parent)
	p := &s3ExtractPipeline{
		parent: parent,
		ctx:    ctx,
		cancel: cancel,
		tasks:  make(chan s3ExtractTask),
	}
	p.wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer p.wg.Done()
			for task := range p.tasks {
				if task == nil {
					continue
				}
				if err := task(p.ctx); err != nil {
					p.storeError(err)
				}
			}
		}()
	}
	return p
}

// Submit queues one task or returns promptly when the pipeline has been canceled.
func (p *s3ExtractPipeline) Submit(task s3ExtractTask) error {
	if task == nil {
		return nil
	}
	select {
	case <-p.ctx.Done():
		return p.resultError()
	case p.tasks <- task:
		return nil
	}
}

// Wait closes the task queue, waits for all workers, and returns the first error.
func (p *s3ExtractPipeline) Wait() error {
	if p == nil {
		return nil
	}
	p.closeOnce.Do(func() {
		close(p.tasks)
	})
	p.wg.Wait()
	p.cancel()
	return p.resultError()
}

// Context exposes the cancellation context shared by all concurrent extract work.
func (p *s3ExtractPipeline) Context() context.Context {
	if p == nil {
		return nil
	}
	return p.ctx
}

// resultError reports the first task error, otherwise the parent context error.
func (p *s3ExtractPipeline) resultError() error {
	if p == nil {
		return nil
	}
	p.errMu.Lock()
	err := p.err
	p.errMu.Unlock()
	if err != nil {
		return err
	}
	if p.parent != nil {
		return p.parent.Err()
	}
	return nil
}

// storeError records the first task error and cancels the worker context.
func (p *s3ExtractPipeline) storeError(err error) {
	if err == nil {
		return
	}
	p.errMu.Lock()
	if p.err == nil {
		p.err = err
		p.cancel()
	}
	p.errMu.Unlock()
}

// s3ExtractStagingBudget bounds the total bytes staged on disk for tar uploads.
type s3ExtractStagingBudget struct {
	total int64

	mu     sync.Mutex
	used   int64
	notify chan struct{}
}

// newS3ExtractStagingBudget returns one staging budget manager.
func newS3ExtractStagingBudget(total int64) *s3ExtractStagingBudget {
	return &s3ExtractStagingBudget{
		total:  total,
		notify: make(chan struct{}),
	}
}

// Fits reports whether a single payload can ever fit within the staging budget.
func (b *s3ExtractStagingBudget) Fits(n int64) bool {
	return b != nil && n >= 0 && n <= b.total
}

// Acquire blocks until the requested staging budget is available or ctx ends.
func (b *s3ExtractStagingBudget) Acquire(ctx context.Context, n int64) error {
	if b == nil || n <= 0 {
		return nil
	}
	for {
		b.mu.Lock()
		if b.used+n <= b.total {
			b.used += n
			b.mu.Unlock()
			return nil
		}
		notify := b.notify
		b.mu.Unlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-notify:
		}
	}
}

// Release returns previously acquired staging bytes to the shared budget.
func (b *s3ExtractStagingBudget) Release(n int64) {
	if b == nil || n <= 0 {
		return
	}
	b.mu.Lock()
	b.used -= n
	if b.used < 0 {
		b.used = 0
	}
	close(b.notify)
	b.notify = make(chan struct{})
	b.mu.Unlock()
}

// stageReaderToTempPath copies reader into a temp file and returns the file path.
func stageReaderToTempPath(ctx context.Context, dir string, pattern string, reader io.Reader) (string, error) {
	if dir == "" {
		dir = os.TempDir()
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	tmp, err := os.CreateTemp(dir, pattern)
	if err != nil {
		return "", err
	}

	path := tmp.Name()
	if _, err := archiveutil.CopyWithContext(ctx, tmp, reader); err != nil {
		closeErr := tmp.Close()
		_ = os.Remove(path)
		if closeErr != nil && !errors.Is(closeErr, os.ErrClosed) {
			return "", errors.Join(err, closeErr)
		}
		return "", err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(path)
		return "", err
	}
	return path, nil
}

// stageReaderToTempFileLimit copies reader into a temp file while enforcing an optional limit.
func stageReaderToTempFileLimit(ctx context.Context, dir string, pattern string, reader io.Reader, limit int64) (*os.File, int64, error) {
	if dir == "" {
		dir = os.TempDir()
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, 0, err
	}
	tmp, err := os.CreateTemp(dir, pattern)
	if err != nil {
		return nil, 0, err
	}
	if _, err := archiveutil.CopyWithContextLimit(ctx, tmp, reader, limit); err != nil {
		path := tmp.Name()
		closeErr := tmp.Close()
		_ = os.Remove(path)
		if closeErr != nil && !errors.Is(closeErr, os.ErrClosed) {
			return nil, 0, errors.Join(err, closeErr)
		}
		return nil, 0, err
	}
	st, err := tmp.Stat()
	if err != nil {
		path := tmp.Name()
		_ = tmp.Close()
		_ = os.Remove(path)
		return nil, 0, err
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		path := tmp.Name()
		_ = tmp.Close()
		_ = os.Remove(path)
		return nil, 0, err
	}
	return tmp, st.Size(), nil
}

// cleanupTempFile closes and removes one staged file.
func cleanupTempFile(file *os.File) error {
	if file == nil {
		return nil
	}
	path := file.Name()
	closeErr := file.Close()
	removeErr := os.Remove(path)
	return errors.Join(closeErr, removeErr)
}

// openStagedFileReader opens one staged file for upload reads.
func openStagedFileReader(path string) (*os.File, error) {
	return os.Open(filepath.Clean(path))
}
