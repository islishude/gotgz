package engine

import (
	"archive/zip"

	"github.com/islishude/gotgz/packages/archivepath"
	"github.com/islishude/gotgz/packages/cli"
)

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
