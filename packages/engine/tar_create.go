package engine

import (
	"context"
	"errors"
	"io/fs"

	"github.com/islishude/gotgz/packages/archiveprogress"
	"github.com/islishude/gotgz/packages/cli"
	"github.com/islishude/gotgz/packages/locator"
)

// runCreateTar writes create-mode output in tar format.
func (r *Runner) runCreateTar(ctx context.Context, opts cli.Options, archiveRef locator.Ref, reporter *archiveprogress.Reporter) (int, error) {
	metadataPolicy, warnings := r.effectiveMetadataPolicy(opts, reporter)

	input, err := r.prepareCreateInput(ctx, opts, archiveRef, reporter)
	if err != nil {
		return 0, err
	}

	tw, err := r.newTarArchiveWriter(ctx, opts, input.archiveRef)
	if err != nil {
		return 0, errors.Join(err, input.source.Close())
	}
	createWarnings, err := input.source.Visit(
		ctx,
		func(ref locator.Ref) error {
			return r.addS3TarMember(ctx, tw, ref, opts.Verbose, reporter)
		},
		func(source localCreateSource) (int, error) {
			return visitLocalCreateSource(ctx, source, func(record localCreateRecord, info fs.FileInfo) (int, error) {
				return r.writeLocalTarRecord(ctx, tw, record, info, opts.Verbose, metadataPolicy, reporter)
			})
		},
	)
	warnings += input.warnings + createWarnings
	cleanupErr := input.source.Close()
	if err != nil || cleanupErr != nil {
		rootErr := errors.Join(err, cleanupErr)
		return warnings, errors.Join(rootErr, tw.Abort(rootErr))
	}
	if err := tw.Close(); err != nil {
		return warnings, err
	}
	return warnings, nil
}
