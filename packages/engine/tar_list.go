package engine

import (
	"archive/tar"
	"context"
	"io"

	"github.com/islishude/gotgz/packages/archivepath"
	"github.com/islishude/gotgz/packages/archiveprogress"
	"github.com/islishude/gotgz/packages/archiveutil"
	"github.com/islishude/gotgz/packages/cli"
	"github.com/islishude/gotgz/packages/locator"
)

// runListTar lists archive members from a tar input stream or split-volume set.
func (r *Runner) runListTar(ctx context.Context, opts cli.Options, reporter *archiveprogress.Reporter, ref locator.Ref, ar io.ReadCloser, info archiveReaderInfo) (int, error) {
	volumes, err := r.resolveArchiveVolumes(ctx, ref, info)
	if err != nil {
		return 0, err
	}
	memberMatcher := archivepath.NewMemberMatcher(opts.Members, opts.Wildcards)

	scan := func(scanReader io.ReadCloser, scanInfo archiveReaderInfo) (int, error) {
		return r.scanTarArchiveFromReader(ctx, opts, reporter, scanInfo, archiveutil.NameHint(ref), scanReader, func(hdr *tar.Header, tr *tar.Reader) (int, error) {
			if archivepath.ShouldSkipMemberWithMatcher(memberMatcher, hdr.Name) {
				if _, err := archiveutil.CopyWithContext(ctx, io.Discard, tr); err != nil {
					return 0, err
				}
				return 0, nil
			}
			reporter.ExternalLinef(r.stdout, "%s\n", hdr.Name)
			if _, err := archiveutil.CopyWithContext(ctx, io.Discard, tr); err != nil {
				return 0, err
			}
			return 0, nil
		})
	}

	if len(volumes) == 1 {
		reporter.SetTotal(info.Size, info.SizeKnown)
		return scan(ar, info)
	}
	return r.scanTarArchiveFromVolumes(ctx, opts, reporter, volumes, ar, scan)
}
