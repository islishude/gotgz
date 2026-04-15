package engine

import (
	"archive/tar"
	"context"
	"errors"
	"io"

	"github.com/islishude/gotgz/packages/archiveprogress"
	"github.com/islishude/gotgz/packages/cli"
	"github.com/islishude/gotgz/packages/compress"
)

// scanTarArchiveFromReader scans a tar stream with optional compression.
func (r *Runner) scanTarArchiveFromReader(ctx context.Context, opts cli.Options, reporter *archiveprogress.Reporter, info archiveReaderInfo, hint string, ar io.ReadCloser, fn func(hdr *tar.Header, tr *tar.Reader) (int, error)) (int, error) {
	ar = archiveprogress.NewCountingReadCloser(ar, reporter)

	cr, _, err := compress.NewReader(ar, compress.FromString(string(opts.Compression)), hint, info.ContentType)
	if err != nil {
		return 0, err
	}
	defer cr.Close() //nolint:errcheck

	tr := tar.NewReader(cr)
	warnings := 0
	for {
		select {
		case <-ctx.Done():
			return warnings, ctx.Err()
		default:
		}
		hdr, err := tr.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return warnings, err
		}
		select {
		case <-ctx.Done():
			return warnings, ctx.Err()
		default:
		}
		w, err := fn(hdr, tr)
		warnings += w
		if err != nil {
			return warnings, err
		}
	}
	return warnings, nil
}

// scanTarArchiveFromVolumes scans a discovered split archive volume-by-volume.
func (r *Runner) scanTarArchiveFromVolumes(ctx context.Context, _ cli.Options, reporter *archiveprogress.Reporter, volumes []archiveVolume, first io.ReadCloser, scan func(io.ReadCloser, archiveReaderInfo) (int, error)) (int, error) {
	var total int64
	totalKnown := true
	for _, volume := range volumes {
		if !volume.info.SizeKnown {
			totalKnown = false
			continue
		}
		total = addArchiveVolumeSize(total, volume.info.Size)
	}
	reporter.SetTotal(total, totalKnown)

	warnings := 0
	for index, volume := range volumes {
		var (
			reader io.ReadCloser
			info   archiveReaderInfo
			err    error
		)

		if index == 0 {
			reader = first
			info = volume.info
		} else {
			reader, info, err = r.openArchiveReader(ctx, volume.ref)
			if err != nil {
				return warnings, err
			}
			info = mergeArchiveReaderInfo(volume.info, info)
		}

		w, err := scan(reader, info)
		warnings += w
		if err != nil {
			return warnings, err
		}
	}
	return warnings, nil
}
