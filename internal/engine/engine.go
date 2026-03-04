package engine

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/islishude/gotgz/internal/cli"
	"github.com/islishude/gotgz/internal/locator"
	httpstore "github.com/islishude/gotgz/internal/storage/http"
	localstore "github.com/islishude/gotgz/internal/storage/local"
	s3store "github.com/islishude/gotgz/internal/storage/s3"
)

const (
	ExitSuccess = 0
	ExitWarning = 1
	ExitFatal   = 2
)

type PermissionPolicy struct {
	SameOwner    bool
	SamePerms    bool
	NumericOwner bool
}

type MetadataPolicy struct {
	Xattrs bool
	ACL    bool
}

type Runner struct {
	local  *localstore.ArchiveStore
	s3     *s3store.Store
	http   *httpstore.Store
	stderr io.Writer
	stdout io.Writer
}

type RunResult struct {
	ExitCode int
	Err      error
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
	return &Runner{
		local:  &localstore.ArchiveStore{},
		s3:     s3s,
		http:   httpstore.New(),
		stdout: stdout,
		stderr: stderr,
	}, nil
}

// Run executes one CLI mode and maps warnings/errors to a process exit code.
func (r *Runner) Run(ctx context.Context, opts cli.Options) RunResult {
	switch opts.Mode {
	case cli.ModeCreate:
		warnings, err := r.runCreate(ctx, opts)
		return classifyResult(err, warnings)
	case cli.ModeExtract:
		warnings, err := r.runExtract(ctx, opts)
		return classifyResult(err, warnings)
	case cli.ModeList:
		warnings, err := r.runList(ctx, opts)
		return classifyResult(err, warnings)
	default:
		return RunResult{ExitCode: ExitFatal, Err: fmt.Errorf("unsupported mode %q", opts.Mode)}
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
func (r *Runner) runCreate(ctx context.Context, opts cli.Options) (warnings int, retErr error) {
	archiveRef, err := locator.ParseArchive(opts.Archive)
	if err != nil {
		return 0, err
	}
	format := detectCreateArchiveFormat(archiveRef)
	switch format {
	case archiveFormatZip:
		return r.runCreateZip(ctx, opts, archiveRef)
	default:
		return r.runCreateTar(ctx, opts, archiveRef)
	}
}

// runList dispatches list mode to tar or zip readers based on archive format.
func (r *Runner) runList(ctx context.Context, opts cli.Options) (int, error) {
	reporter := newProgressReporter(r.stderr, opts.Progress, 0, false, time.Now(), true)
	defer reporter.Finish()
	ref, ar, info, magic, err := r.openArchiveForRead(ctx, opts.Archive)
	if err != nil {
		return 0, err
	}
	defer ar.Close() //nolint:errcheck

	switch detectReadArchiveFormat(magic, archiveNameHint(ref), info.ContentType) {
	case archiveFormatZip:
		return r.runListZip(ctx, opts, reporter, ref, ar, info)
	default:
		return r.runListTar(ctx, opts, reporter, ar, info)
	}
}

// runExtract dispatches extract mode to tar or zip readers based on archive format.
func (r *Runner) runExtract(ctx context.Context, opts cli.Options) (int, error) {
	reporter := newProgressReporter(r.stderr, opts.Progress, 0, false, time.Now(), opts.Verbose && !opts.ToStdout)
	defer reporter.Finish()
	ref, ar, info, magic, err := r.openArchiveForRead(ctx, opts.Archive)
	if err != nil {
		return 0, err
	}
	defer ar.Close() //nolint:errcheck

	switch detectReadArchiveFormat(magic, archiveNameHint(ref), info.ContentType) {
	case archiveFormatZip:
		return r.runExtractZip(ctx, opts, reporter, ref, ar, info)
	default:
		return r.runExtractTar(ctx, opts, reporter, ar, info)
	}
}
