package engine

import (
	"context"
	"io/fs"

	"github.com/islishude/gotgz/packages/archiveprogress"
	"github.com/islishude/gotgz/packages/cli"
	"github.com/islishude/gotgz/packages/locator"
)

// runCreateZip writes create-mode output in zip format.
func (r *Runner) runCreateZip(ctx context.Context, opts cli.Options, archiveRef locator.Ref, reporter *archiveprogress.Reporter) (warnings int, retErr error) {
	warnings += r.warnZipCreateOptions(opts, reporter)

	input, err := r.prepareCreateInput(ctx, opts, archiveRef, reporter)
	if err != nil {
		return warnings, err
	}

	zw, err := r.newZipArchiveWriter(ctx, opts, input.archiveRef)
	if err != nil {
		return warnings, err
	}
	defer func() {
		if cerr := zw.Close(); cerr != nil && retErr == nil {
			retErr = cerr
		}
	}()

	createWarnings, err := input.source.Visit(
		ctx,
		func(ref locator.Ref) error {
			return r.addS3ZipMember(ctx, zw, ref, opts.Verbose, reporter)
		},
		func(source localCreateSource) (int, error) {
			return visitLocalCreateSource(ctx, source, func(record localCreateRecord, info fs.FileInfo) (int, error) {
				return r.writeLocalZipRecord(ctx, zw, record, info, opts.Verbose, reporter)
			})
		},
	)
	return warnings + createWarnings, err
}
