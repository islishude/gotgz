package engine

import (
	"context"
	"errors"

	"github.com/islishude/gotgz/packages/archiveprogress"
	"github.com/islishude/gotgz/packages/cli"
	"github.com/islishude/gotgz/packages/locator"
)

type createArchiveWriter interface {
	Close() error
	Abort(error) error
}

// createArchiveOperation supplies format-specific entry encoders. The shared
// runner owns input cleanup, failure rollback, and final archive publication.
type createArchiveOperation struct {
	writer      createArchiveWriter
	handleS3    func(locator.Ref) error
	handleLocal func(localCreateSource) (int, error)
}

func (r *Runner) runCreateWithWriter(ctx context.Context, opts cli.Options, archiveRef locator.Ref, reporter *archiveprogress.Reporter, warnings int, open func() (createArchiveOperation, error)) (int, error) {
	input, err := r.prepareCreateInput(ctx, opts, archiveRef, reporter)
	if err != nil {
		return warnings, err
	}
	warnings += input.warnings
	op, err := open()
	if err != nil {
		return warnings, errors.Join(err, input.source.Close())
	}
	if err := input.registerWriterArtifacts(op.writer); err != nil {
		cause := errors.Join(err, input.source.Close())
		return warnings, errors.Join(cause, op.writer.Abort(cause))
	}
	reporter.BeginPayload()
	w, err := input.source.Visit(ctx, op.handleS3, op.handleLocal)
	warnings += w
	if err == nil && input.streamingOutputWasSkipped() {
		warnings += r.warnf(reporter, "create: archive output inside an input tree was skipped")
	}
	if cause := errors.Join(err, input.source.Close()); cause != nil {
		return warnings, errors.Join(cause, op.writer.Abort(cause))
	}
	return warnings, op.writer.Close()
}
