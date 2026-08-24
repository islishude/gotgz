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

type streamingCreateMember struct {
	task  createPlanTask
	spool *streamingMemberSpool
}

type streamingProducerResult struct {
	total int64
	err   error
}

// streamingCreateInputSource overlaps recursive local planning with archive
// replay while retaining one disk-backed spool per top-level member.
type streamingCreateInputSource struct {
	request       preparedCreateRequest
	reporter      createTotalReporter
	spoolDir      string
	spoolInfo     fs.FileInfo
	members       []streamingCreateMember
	scannerConfig createPlanScannerConfig

	stateMu      sync.Mutex
	started      bool
	cancel       context.CancelCauseFunc
	producerDone chan struct{}

	closeOnce sync.Once
	closeErr  error
}

func newStreamingCreateInputSource(request preparedCreateRequest, reporter createTotalReporter) (_ *streamingCreateInputSource, retErr error) {
	spoolDir, err := os.MkdirTemp("", "gotgz-create-stream-*")
	if err != nil {
		return nil, fmt.Errorf("create streaming plan spool directory: %w", err)
	}
	if err := os.Chmod(spoolDir, 0o700); err != nil {
		_ = os.RemoveAll(spoolDir)
		return nil, fmt.Errorf("secure streaming plan spool directory: %w", err)
	}
	spoolInfo, err := os.Stat(spoolDir)
	if err != nil {
		_ = os.RemoveAll(spoolDir)
		return nil, fmt.Errorf("stat streaming plan spool directory: %w", err)
	}

	limiter := newCreatePlanMetadataLimiter(defaultCreatePlanMetadataConcurrency())
	source := &streamingCreateInputSource{
		request:       request,
		reporter:      reporter,
		spoolDir:      spoolDir,
		spoolInfo:     spoolInfo,
		members:       make([]streamingCreateMember, 0, len(request.tasks)),
		scannerConfig: newCreatePlanScannerConfig(limiter),
	}
	defer func() {
		if retErr != nil {
			retErr = errors.Join(retErr, source.Close())
		}
	}()

	for _, task := range request.tasks {
		if task.ref.Kind != locator.KindLocal {
			return nil, fmt.Errorf("streaming create plan does not support input %q", task.member)
		}
		spool, err := newStreamingMemberSpool(spoolDir)
		if err != nil {
			return nil, err
		}
		source.members = append(source.members, streamingCreateMember{task: task, spool: spool})
	}
	return source, nil
}

func (*streamingCreateInputSource) Total() (int64, bool) {
	return 0, false
}

func (s *streamingCreateInputSource) Visit(ctx context.Context, _ func(ref locator.Ref) error, handleLocal func(source localCreateSource) (int, error)) (int, error) {
	producerDone, cancel, err := s.start(ctx)
	if err != nil {
		return 0, err
	}

	warnings := 0
	for _, member := range s.members {
		select {
		case <-ctx.Done():
			cancel(ctx.Err())
			<-producerDone
			return warnings, ctx.Err()
		default:
		}
		memberWarnings, err := handleLocal(streamingLocalCreateSource{spool: member.spool})
		warnings += memberWarnings
		if err != nil {
			cancel(err)
			<-producerDone
			return warnings, err
		}
	}
	<-producerDone
	return warnings, nil
}

func (s *streamingCreateInputSource) start(ctx context.Context) (<-chan struct{}, context.CancelCauseFunc, error) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.started {
		return nil, nil, fmt.Errorf("streaming create source can only be visited once")
	}
	s.started = true
	workCtx, cancel := context.WithCancelCause(ctx)
	s.cancel = cancel
	s.producerDone = make(chan struct{})
	producerDone := s.producerDone
	results := make(chan streamingProducerResult, len(s.members))

	var producers sync.WaitGroup
	for _, member := range s.members {
		producers.Go(func() {
			total, _, err := scanLocalCreateRecords(
				workCtx,
				member.task.member,
				s.request.opts.Chdir,
				s.request.excludeMatcher,
				s.request.outputPolicy,
				s.spoolInfo,
				member.spool,
				s.scannerConfig,
			)
			member.spool.Finish(total, err)
			results <- streamingProducerResult{total: total, err: err}
		})
	}
	go func() {
		producers.Wait()
		close(results)
		total := int64(0)
		allComplete := true
		for result := range results {
			total = addCreatePlanSize(total, result.total)
			if result.err != nil {
				allComplete = false
			}
		}
		if allComplete && s.reporter != nil {
			s.reporter.SetTotal(total, true)
		}
		close(producerDone)
	}()
	return producerDone, cancel, nil
}

func (s *streamingCreateInputSource) Close() error {
	if s == nil {
		return nil
	}
	s.closeOnce.Do(func() {
		s.stateMu.Lock()
		cancel := s.cancel
		producerDone := s.producerDone
		s.stateMu.Unlock()
		if cancel != nil {
			cancel(context.Canceled)
		}
		if producerDone != nil {
			<-producerDone
		}

		var errs []error
		for _, member := range s.members {
			errs = append(errs, member.spool.Close())
		}
		if err := os.RemoveAll(s.spoolDir); err != nil {
			errs = append(errs, fmt.Errorf("remove streaming plan spool directory %q: %w", s.spoolDir, err))
		}
		s.closeErr = errors.Join(errs...)
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
