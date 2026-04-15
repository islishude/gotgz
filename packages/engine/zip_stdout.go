package engine

import (
	"archive/zip"
	"context"
	"errors"
	"io"

	"github.com/islishude/gotgz/packages/archivepath"
	"github.com/islishude/gotgz/packages/archiveprogress"
	"github.com/islishude/gotgz/packages/archiveutil"
	"github.com/islishude/gotgz/packages/cli"
)

// extractZipToStdout writes matching regular zip members to stdout.
func (r *Runner) extractZipToStdout(ctx context.Context, zr *zip.Reader, memberMatcher *archivepath.CompiledPathMatcher, opts cli.Options, reporter *archiveprogress.Reporter) (int, error) {
	warnings := 0
	for _, zf := range zr.File {
		select {
		case <-ctx.Done():
			return warnings, ctx.Err()
		default:
		}
		if archivepath.ShouldSkipMemberWithMatcher(memberMatcher, zf.Name) {
			continue
		}
		name, ok := archivepath.StripPathComponents(zf.Name, opts.StripComponents)
		if !ok || name == "" || !isZipRegular(zf) {
			continue
		}
		rc, w, err := r.openZipEntry(zf, reporter)
		warnings += w
		if err != nil {
			return warnings, err
		}
		if rc == nil {
			continue
		}
		_, err = archiveutil.CopyWithContext(ctx, r.stdout, archiveprogress.NewCountingReader(rc, reporter))
		cerr := rc.Close()
		if err != nil {
			return warnings, err
		}
		if cerr != nil {
			return warnings, cerr
		}
	}
	return warnings, nil
}

// openZipEntry opens one zip file entry and downgrades unsupported algorithms
// into warnings so extraction/list can continue.
func (r *Runner) openZipEntry(zf *zip.File, reporter *archiveprogress.Reporter) (io.ReadCloser, int, error) {
	rc, err := zf.Open()
	if err == nil {
		return rc, 0, nil
	}
	if errors.Is(err, zip.ErrAlgorithm) {
		return nil, r.warnf(reporter, "zip entry %s uses unsupported algorithm/encryption; skipping", zf.Name), nil
	}
	return nil, 0, err
}
