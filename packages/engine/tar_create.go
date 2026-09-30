package engine

import (
	"context"

	"github.com/islishude/gotgz/packages/archiveprogress"
	"github.com/islishude/gotgz/packages/cli"
	"github.com/islishude/gotgz/packages/locator"
)

// runCreateTar writes create-mode output in tar format.
func (r *Runner) runCreateTar(ctx context.Context, opts cli.Options, archiveRef locator.Ref, reporter *archiveprogress.Reporter) (int, error) {
	metadataPolicy, warnings := r.effectiveMetadataPolicy(opts, reporter)

	return r.runCreateWithWriter(ctx, opts, archiveRef, reporter, warnings, func() (createArchiveOperation, error) {
		writer, err := r.newTarArchiveWriter(ctx, opts, archiveRef)
		if err != nil {
			return createArchiveOperation{}, err
		}
		return createArchiveOperation{
			writer: writer,
			handleS3: func(ref locator.Ref) error {
				return r.addS3TarMember(ctx, writer, ref, opts.Verbose, reporter)
			},
			handleLocal: func(source localCreateSource) (int, error) {
				return visitLocalCreateSource(ctx, source, func(entry *localEntryHandle) (int, error) {
					return r.writeLocalTarRecord(ctx, writer, entry, opts.Verbose, metadataPolicy, reporter)
				})
			},
		}, nil
	})
}
