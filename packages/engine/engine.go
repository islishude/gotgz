package engine

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/islishude/gotgz/packages/archivepath"
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
	storage *storageRouter
	stderr  io.Writer
	stdout  io.Writer
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
		storage: &storageRouter{local: local, s3: s3, http: http},
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

// processCreateMembers parses create-mode members once and dispatches them by backend kind.
func (r *Runner) processCreateMembers(ctx context.Context, opts cli.Options, excludeMatcher *archivepath.CompiledPathMatcher, handleS3 func(ref locator.Ref) error, handleLocal func(member string) (int, error)) (int, error) {
	warnings := 0
	for _, member := range opts.Members {
		select {
		case <-ctx.Done():
			return warnings, ctx.Err()
		default:
		}

		ref, err := locator.ParseMember(member)
		if err != nil {
			return warnings, err
		}

		switch ref.Kind {
		case locator.KindS3:
			if archivepath.MatchExcludeWithMatcher(excludeMatcher, ref.Key) {
				continue
			}
			if err := handleS3(ref); err != nil {
				return warnings, err
			}
		case locator.KindLocal:
			w, err := handleLocal(member)
			warnings += w
			if err != nil {
				return warnings, err
			}
		default:
			return warnings, fmt.Errorf("unsupported member reference %q", member)
		}
	}
	return warnings, nil
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
