package engine

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/islishude/gotgz/packages/archiveprogress"
	"github.com/islishude/gotgz/packages/archiveutil"
	"github.com/islishude/gotgz/packages/cli"
	"github.com/islishude/gotgz/packages/locator"
	httpstore "github.com/islishude/gotgz/packages/storage/http"
	localstore "github.com/islishude/gotgz/packages/storage/local"
	s3store "github.com/islishude/gotgz/packages/storage/s3"
)

const (
	ExitSuccess = 0
	ExitWarning = 1
	ExitFatal   = 2
)

type PermissionPolicy = cli.PermissionPolicy

type MetadataPolicy = cli.MetadataPolicy

type Runner struct {
	storage           *storageRouter
	stderr            io.Writer
	stdout            io.Writer
	outputMu          sync.Mutex
	splitExtractHooks *splitExtractHooks
}

// splitExtractHooks exposes internal split-extract lifecycle callbacks for tests.
type splitExtractHooks struct {
	onPlan                func(splitExtractPlan)
	onParallelWorkerStart func(int, locator.Ref)
}

// RunResult summarizes one completed CLI operation together with progress metadata.
type RunResult struct {
	ExitCode        int
	Err             error
	Elapsed         time.Duration
	ProgressEnabled bool
}

// archiveReaderInfo holds metadata returned alongside an opened archive reader.
type archiveReaderInfo struct {
	Size        int64
	SizeKnown   bool
	ContentType string
}

// New creates a Runner with all supported storage backends initialized.
func New(ctx context.Context, stdout io.Writer, stderr io.Writer) (*Runner, error) {
	s3s, err := s3store.New(ctx)
	if err != nil {
		return nil, fmt.Errorf("init s3: %w", err)
	}
	return newRunner(
		&localstore.ArchiveStore{},
		s3s,
		httpstore.New(),
		stdout,
		stderr,
	), nil
}

// newRunner wires a Runner from injected storage backends.
func newRunner(local localArchiveStore, s3 s3ArchiveStore, http httpArchiveStore, stdout io.Writer, stderr io.Writer) *Runner {
	return &Runner{
		storage: newStorageRouter(local, s3, http),
		stdout:  stdout,
		stderr:  stderr,
	}
}

// Run executes one CLI mode and maps warnings/errors to a process exit code.
func (r *Runner) Run(ctx context.Context, opts cli.Options) RunResult {
	reporter := newRunReporter(r.stderr, opts)
	if reporter == nil {
		return RunResult{ExitCode: ExitFatal, Err: fmt.Errorf("unsupported mode %q", opts.Mode)}
	}

	var warnings int
	var err error
	switch opts.Mode {
	case cli.ModeCreate:
		warnings, err = r.runCreate(ctx, opts, reporter)
	case cli.ModeExtract:
		warnings, err = r.runExtract(ctx, opts, reporter)
	case cli.ModeList:
		warnings, err = r.runList(ctx, opts, reporter)
	default:
		return RunResult{ExitCode: ExitFatal, Err: fmt.Errorf("unsupported mode %q", opts.Mode)}
	}

	reporter.Finish()
	result := classifyResult(err, warnings)
	result.Elapsed = reporter.Elapsed()
	result.ProgressEnabled = reporter.Enabled()
	return result
}

// newRunReporter creates the reporter shared by the entire engine run.
func newRunReporter(stderr io.Writer, opts cli.Options) *archiveprogress.Reporter {
	switch opts.Mode {
	case cli.ModeCreate:
		return archiveprogress.NewReporter(stderr, opts.Progress, 0, false, time.Now(), opts.Verbose)
	case cli.ModeExtract:
		return archiveprogress.NewReporter(stderr, opts.Progress, 0, false, time.Now(), opts.Verbose && !opts.ToStdout)
	case cli.ModeList:
		return archiveprogress.NewReporter(stderr, opts.Progress, 0, false, time.Now(), true)
	default:
		return nil
	}
}

// classifyResult turns warning/error outcomes into a RunResult.
func classifyResult(err error, warnings int) RunResult {
	if err != nil {
		return RunResult{ExitCode: ExitFatal, Err: err}
	}
	if warnings > 0 {
		return RunResult{ExitCode: ExitWarning}
	}
	return RunResult{ExitCode: ExitSuccess}
}

// runCreate dispatches create mode to the archive-format specific implementation.
func (r *Runner) runCreate(ctx context.Context, opts cli.Options, reporter *archiveprogress.Reporter) (warnings int, retErr error) {
	archiveRef, err := locator.ParseArchive(opts.Archive)
	if err != nil {
		return 0, err
	}
	archiveRef = archiveRef.WithS3CacheControl(opts.S3CacheControl)
	archiveRef = archiveRef.WithS3ObjectTags(opts.S3ObjectTags)
	format := archiveutil.DetectCreateArchiveFormat(archiveRef)
	switch format {
	case archiveutil.ArchiveFormatZip:
		return r.runCreateZip(ctx, opts, archiveRef, reporter)
	case archiveutil.ArchiveFormatTar:
		return r.runCreateTar(ctx, opts, archiveRef, reporter)
	default:
		return 0, fmt.Errorf("cannot determine archive format for %q; consider using -suffix", opts.Archive)
	}
}

// runList dispatches list mode to tar or zip readers based on archive format.
func (r *Runner) runList(ctx context.Context, opts cli.Options, reporter *archiveprogress.Reporter) (int, error) {
	ref, ar, info, magic, err := r.openArchiveForRead(ctx, opts.Archive)
	if err != nil {
		return 0, err
	}
	defer ar.Close() //nolint:errcheck

	switch archiveutil.DetectReadArchiveFormat(magic, archiveutil.NameHint(ref), info.ContentType) {
	case archiveutil.ArchiveFormatZip:
		return r.runListZip(ctx, opts, reporter, ref, ar, info)
	case archiveutil.ArchiveFormatTar:
		return r.runListTar(ctx, opts, reporter, ref, ar, info)
	default:
		return 0, fmt.Errorf("cannot determine archive format for %q; consider using -suffix", opts.Archive)
	}
}

// runExtract dispatches extract mode to tar or zip readers based on archive format.
func (r *Runner) runExtract(ctx context.Context, opts cli.Options, reporter *archiveprogress.Reporter) (int, error) {
	ref, ar, info, magic, err := r.openArchiveForRead(ctx, opts.Archive)
	if err != nil {
		return 0, err
	}
	defer ar.Close() //nolint:errcheck

	switch archiveutil.DetectReadArchiveFormat(magic, archiveutil.NameHint(ref), info.ContentType) {
	case archiveutil.ArchiveFormatZip:
		return r.runExtractZip(ctx, opts, reporter, ref, ar, info)
	case archiveutil.ArchiveFormatTar:
		return r.runExtractTar(ctx, opts, reporter, ref, ar, info)
	default:
		return 0, fmt.Errorf("cannot determine archive format for %q; consider using -suffix", opts.Archive)
	}
}

// dispatchExtractTarget routes one normalized archive entry to the resolved extract target.
func (r *Runner) dispatchExtractTarget(target locator.Ref, targetArg string, extractToS3 func(target locator.Ref) (int, error), extractToLocal func(base string) (int, error)) (int, error) {
	switch target.Kind {
	case locator.KindS3:
		return extractToS3(target)
	case locator.KindLocal, locator.KindStdio:
		return extractToLocal(target.Path)
	default:
		return 0, fmt.Errorf("unsupported extract target %q", targetArg)
	}
}

// writeOutputLineLocked serializes one external line write together with progress state changes.
func (r *Runner) writeOutputLineLocked(writer io.Writer, reporter *archiveprogress.Reporter, format string, args ...any) {
	r.outputMu.Lock()
	defer r.outputMu.Unlock()

	if reporter != nil {
		reporter.BeforeExternalLineOutput()
	}
	_, _ = fmt.Fprintf(writer, format, args...)
	if reporter != nil {
		reporter.AfterExternalLineOutput()
	}
}

// notifySplitExtractPlan invokes the optional split-extract plan hook for tests.
func (r *Runner) notifySplitExtractPlan(plan splitExtractPlan) {
	if r == nil || r.splitExtractHooks == nil || r.splitExtractHooks.onPlan == nil {
		return
	}
	r.splitExtractHooks.onPlan(plan)
}

// notifySplitExtractWorkerStart invokes the optional split-extract worker hook for tests.
func (r *Runner) notifySplitExtractWorkerStart(volumeIndex int, ref locator.Ref) {
	if r == nil || r.splitExtractHooks == nil || r.splitExtractHooks.onParallelWorkerStart == nil {
		return
	}
	r.splitExtractHooks.onParallelWorkerStart(volumeIndex, ref)
}
