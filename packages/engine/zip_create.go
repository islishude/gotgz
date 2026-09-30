package engine

import (
	"context"

	"github.com/islishude/gotgz/packages/archiveprogress"
	"github.com/islishude/gotgz/packages/cli"
	"github.com/islishude/gotgz/packages/locator"
)

// runCreateZip writes create-mode output in zip format.
func (r *Runner) runCreateZip(ctx context.Context, opts cli.Options, archiveRef locator.Ref, reporter *archiveprogress.Reporter) (int, error) {
	warnings := r.warnZipCreateOptions(opts, reporter)

	return r.runCreateWithWriter(ctx, opts, archiveRef, reporter, warnings, func() (createArchiveOperation, error) {
		writer, err := r.newZipArchiveWriter(ctx, opts, archiveRef)
		if err != nil {
			return createArchiveOperation{}, err
		}
		return createArchiveOperation{
			writer: writer,
			handleS3: func(ref locator.Ref) error {
				return r.addS3ZipMember(ctx, writer, ref, opts.Verbose, reporter)
			},
			handleLocal: func(source localCreateSource) (int, error) {
				return visitLocalCreateSource(ctx, source, func(entry *localEntryHandle) (int, error) {
					return r.writeLocalZipRecord(ctx, writer, entry, opts.Verbose, reporter)
				})
			},
		}, nil
	})
}
