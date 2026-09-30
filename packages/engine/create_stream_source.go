package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"sync"

	"github.com/islishude/gotgz/packages/locator"
)

type createTotalReporter interface {
	SetTotal(total int64, known bool)
}

// Bound open spool files and scanners independently of the number of CLI inputs.
const maxStreamingCreateMembers = 8

type streamingCreateMember struct {
	spool *streamingMemberSpool
	done  <-chan struct{}
	err   error
}

func (m streamingCreateMember) close() error {
	if m.done != nil {
		<-m.done
	}
	return m.spool.Close()
}

// streamingCreateInputSource plans ahead through a bounded window. A slot is
// released only after its member has been consumed and its spool removed.
type streamingCreateInputSource struct {
	request       preparedCreateRequest
	reporter      createTotalReporter
	spoolDir      string
	spoolInfo     fs.FileInfo
	scannerConfig createPlanScannerConfig

	stateMu   sync.Mutex
	started   bool
	closed    bool
	cancel    context.CancelCauseFunc
	visitDone chan struct{}

	closeOnce sync.Once
	closeErr  error
}

func newStreamingCreateInputSource(request preparedCreateRequest, reporter createTotalReporter) (*streamingCreateInputSource, error) {
	for _, task := range request.tasks {
		if task.ref.Kind != locator.KindLocal {
			return nil, fmt.Errorf("streaming create plan does not support input %q", task.member)
		}
	}
	spoolDir, err := os.MkdirTemp("", "gotgz-create-stream-*")
	if err != nil {
		return nil, fmt.Errorf("create streaming plan spool directory: %w", err)
	}
	if err := os.Chmod(spoolDir, 0o700); err != nil {
		return nil, errors.Join(fmt.Errorf("secure streaming plan spool directory: %w", err), os.RemoveAll(spoolDir))
	}
	spoolInfo, err := os.Stat(spoolDir)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("stat streaming plan spool directory: %w", err), os.RemoveAll(spoolDir))
	}
	limiter := newCreatePlanMetadataLimiter(defaultCreatePlanMetadataConcurrency())
	return &streamingCreateInputSource{
		request: request, reporter: reporter, spoolDir: spoolDir, spoolInfo: spoolInfo,
		scannerConfig: newCreatePlanScannerConfig(limiter),
	}, nil
}

func (*streamingCreateInputSource) Total() (int64, bool) { return 0, false }

func (s *streamingCreateInputSource) Visit(ctx context.Context, _ func(ref locator.Ref) error, handleLocal func(source localCreateSource) (int, error)) (warnings int, retErr error) {
	slots := make(chan struct{}, maxStreamingCreateMembers)
	workCtx, members, err := s.start(ctx, slots)
	if err != nil {
		return 0, err
	}
	defer func() {
		s.cancel(context.Canceled)
		// Drain every scheduled member after cancellation; producers finish before
		// their files are closed, including members the writer never reached.
		for member := range members {
			retErr = errors.Join(retErr, member.close())
		}
		close(s.visitDone)
	}()
	for member := range members {
		if err := context.Cause(workCtx); err != nil {
			return warnings, errors.Join(err, member.close())
		}
		if member.err != nil {
			return warnings, member.err
		}
		w, err := handleLocal(streamingLocalCreateSource{spool: member.spool})
		warnings += w
		if err != nil {
			s.cancel(err)
			return warnings, errors.Join(err, member.close())
		}
		if err := member.close(); err != nil {
			return warnings, err
		}
		<-slots
	}
	return warnings, context.Cause(workCtx)
}

func (s *streamingCreateInputSource) start(ctx context.Context, slots chan struct{}) (context.Context, <-chan streamingCreateMember, error) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.closed {
		return nil, nil, fmt.Errorf("streaming create source is closed")
	}
	if s.started {
		return nil, nil, fmt.Errorf("streaming create source can only be visited once")
	}
	s.started = true
	workCtx, cancel := context.WithCancelCause(ctx)
	s.cancel = cancel
	s.visitDone = make(chan struct{})
	members := make(chan streamingCreateMember, maxStreamingCreateMembers)
	go s.produce(workCtx, slots, members)
	return workCtx, members, nil
}

func (s *streamingCreateInputSource) produce(ctx context.Context, slots chan struct{}, members chan<- streamingCreateMember) {
	defer close(members)
	var workers sync.WaitGroup
	var totalsMu sync.Mutex
	var total int64
	var completed int
	defer func() {
		workers.Wait()
		if completed == len(s.request.tasks) && context.Cause(ctx) == nil && s.reporter != nil {
			s.reporter.SetTotal(total, true)
		}
	}()
	for _, task := range s.request.tasks {
		select {
		case slots <- struct{}{}:
		case <-ctx.Done():
			return
		}
		spool, err := newStreamingMemberSpool(s.spoolDir)
		if err != nil {
			members <- streamingCreateMember{err: err}
			return
		}
		done := make(chan struct{})
		workers.Go(func() {
			defer close(done)
			size, _, err := scanLocalCreateRecords(ctx, task.member, s.request.opts.Chdir,
				s.request.excludeMatcher, s.request.outputPolicy, s.spoolInfo, spool, s.scannerConfig)
			spool.Finish(size, err)
			if err == nil {
				totalsMu.Lock()
				total = addCreatePlanSize(total, size)
				completed++
				totalsMu.Unlock()
			}
		})
		// Every queued or currently consumed member owns a slot, so this bounded
		// channel always has room, even if cancellation stops the consumer.
		members <- streamingCreateMember{spool: spool, done: done}
	}
}

func (s *streamingCreateInputSource) Close() error {
	if s == nil {
		return nil
	}
	s.closeOnce.Do(func() {
		s.stateMu.Lock()
		s.closed = true
		cancel, done := s.cancel, s.visitDone
		s.stateMu.Unlock()
		if cancel != nil {
			cancel(context.Canceled)
		}
		if done != nil {
			<-done
		}
		if err := os.RemoveAll(s.spoolDir); err != nil {
			s.closeErr = fmt.Errorf("remove streaming plan spool directory %q: %w", s.spoolDir, err)
		}
	})
	return s.closeErr
}

type streamingLocalCreateSource struct {
	spool planTailReader
}

// Visit tails planned records and deliberately refreshes metadata immediately
// before the archive writer sees each entry.
func (s streamingLocalCreateSource) Visit(ctx context.Context, visit func(entry *localEntryHandle) error) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		record, err := s.spool.Next(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		localRecord := localCreateRecord{current: record.Current, archiveName: record.ArchiveName}
		entry, err := openLocalEntry(localRecord, record.EntryType)
		if err != nil {
			return err
		}
		if err := visitLocalEntry(entry, visit); err != nil {
			return err
		}
	}
}
