package engine

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

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
	storage *storageRouter
	stderr  io.Writer
	stdout  io.Writer
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
	archiveRef = applyS3CacheControl(archiveRef, opts.S3CacheControl)
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

	switch detectReadArchiveFormat(magic, archiveutil.NameHint(ref), info.ContentType) {
	case archiveFormatZip:
		return r.runListZip(ctx, opts, reporter, ref, ar, info)
	default:
		return r.runListTar(ctx, opts, reporter, ref, ar, info)
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

	switch detectReadArchiveFormat(magic, archiveutil.NameHint(ref), info.ContentType) {
	case archiveFormatZip:
		return r.runExtractZip(ctx, opts, reporter, ref, ar, info)
	default:
		return r.runExtractTar(ctx, opts, reporter, ref, ar, info)
	}
}

// processCreateMembers parses create-mode members once and dispatches them by backend kind.
func (r *Runner) processCreateMembers(ctx context.Context, opts cli.Options, excludeMatcher *compiledPathMatcher, handleS3 func(ref locator.Ref) error, handleLocal func(member string) (int, error)) (int, error) {
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
			if matchExcludeWithMatcher(excludeMatcher, ref.Key) {
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

// applyArchiveSuffix rewrites archive destinations when create mode uses -suffix.
func applyArchiveSuffix(ref locator.Ref, suffix string) (locator.Ref, error) {
	if suffix == "" {
		return ref, nil
	}

	switch ref.Kind {
	case locator.KindLocal:
		ref.Path = AddArchiveSuffix(ref.Path, suffix)
		ref.Raw = ref.Path
	case locator.KindS3:
		ref.Key = AddArchiveSuffix(ref.Key, suffix)
	case locator.KindStdio:
		return locator.Ref{}, fmt.Errorf("cannot use -suffix with -f -")
	}
	return ref, nil
}

// parseExtractTarget resolves the output target for extract mode and applies S3 options.
func parseExtractTarget(chdir string, cacheControl string) (locator.Ref, error) {
	target := chdir
	if target == "" {
		target = "."
	}
	ref, err := locator.ParseArchive(target)
	if err != nil {
		return locator.Ref{}, err
	}
	return applyS3CacheControl(ref, cacheControl), nil
}

// applyS3CacheControl sets Cache-Control on S3 refs when the option is provided.
func applyS3CacheControl(ref locator.Ref, cacheControl string) locator.Ref {
	if ref.Kind != locator.KindS3 {
		return ref
	}
	cacheControl = strings.TrimSpace(cacheControl)
	if cacheControl == "" {
		return ref
	}
	ref.CacheControl = cacheControl
	return ref
}
