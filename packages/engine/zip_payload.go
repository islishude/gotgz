package engine

import (
	"archive/zip"
	"context"
	"io"

	"github.com/islishude/gotgz/packages/archivepath"
	"github.com/islishude/gotgz/packages/cli"
	"github.com/islishude/gotgz/packages/locator"
)

// sumSplitZipPayloadBytes pre-scans split zip volumes to compute one extraction total.
func (r *Runner) sumSplitZipPayloadBytes(ctx context.Context, volumes []archiveVolume, first io.ReadCloser, firstInfo archiveReaderInfo, match func(zf *zip.File) bool) (int64, error) {
	var total int64
	_, err := r.forEachArchiveVolume(ctx, volumes, first, firstInfo, func(ref locator.Ref, reader io.ReadCloser, readerInfo archiveReaderInfo) (int, error) {
		_, err := r.withZipReader(ctx, ref, reader, readerInfo, nil, func(zr *zip.Reader) (int, error) {
			total = addArchiveVolumeSize(total, totalZipPayloadBytes(zr, match))
			return 0, nil
		})
		return 0, err
	})
	return total, err
}

// matchingZipStdoutPayloadBytes sums uncompressed bytes that would be written to stdout.
func matchingZipStdoutPayloadBytes(zr *zip.Reader, memberMatcher *archivepath.CompiledPathMatcher, opts cli.Options) int64 {
	return totalZipPayloadBytes(zr, func(zf *zip.File) bool {
		return shouldIncludeZipStdoutEntry(zf, memberMatcher, opts.StripComponents)
	})
}

// matchingZipExtractPayloadBytes sums uncompressed bytes that would be extracted.
func matchingZipExtractPayloadBytes(zr *zip.Reader, memberMatcher *archivepath.CompiledPathMatcher, stripComponents int) int64 {
	return totalZipPayloadBytes(zr, func(zf *zip.File) bool {
		return shouldIncludeZipExtractEntry(zf, memberMatcher, stripComponents)
	})
}

// shouldIncludeZipStdoutEntry reports whether one zip entry would be extracted to stdout.
func shouldIncludeZipStdoutEntry(zf *zip.File, memberMatcher *archivepath.CompiledPathMatcher, stripComponents int) bool {
	if archivepath.ShouldSkipMemberWithMatcher(memberMatcher, zf.Name) {
		return false
	}
	name, ok := archivepath.StripPathComponents(zf.Name, stripComponents)
	if !ok || name == "" {
		return false
	}
	return isZipRegular(zf)
}

// shouldIncludeZipExtractEntry reports whether one zip entry would be extracted to a directory or S3 target.
func shouldIncludeZipExtractEntry(zf *zip.File, memberMatcher *archivepath.CompiledPathMatcher, stripComponents int) bool {
	if archivepath.ShouldSkipMemberWithMatcher(memberMatcher, zf.Name) {
		return false
	}
	name, ok := archivepath.StripPathComponents(zf.Name, stripComponents)
	return ok && name != ""
}
