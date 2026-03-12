package engine

import (
	"archive/zip"
	"compress/flate"
	"context"
	"fmt"
	"io"

	"github.com/islishude/gotgz/packages/cli"
	"github.com/islishude/gotgz/packages/locator"
)

const splitZipEntryFinalizeLookaheadBytes int64 = 32

// zipArchiveWriter writes zip entries and lets callers rotate only at member boundaries.
type zipArchiveWriter interface {
	CreateHeader(hdr *zip.FileHeader) (io.Writer, error)
	FinishEntry() error
	Close() error
}

// newZipArchiveWriter returns either the legacy single-file writer or a split-volume writer.
func (r *Runner) newZipArchiveWriter(ctx context.Context, opts cli.Options, archiveRef locator.Ref) (zipArchiveWriter, error) {
	if opts.SplitSizeBytes <= 0 {
		return r.newSingleZipArchiveWriter(ctx, opts, archiveRef)
	}
	return r.newSplitZipArchiveWriter(ctx, opts, archiveRef)
}

// newSingleZipArchiveWriter creates the existing single-stream zip output pipeline.
func (r *Runner) newSingleZipArchiveWriter(ctx context.Context, opts cli.Options, archiveRef locator.Ref) (zipArchiveWriter, error) {
	aw, err := r.openArchiveWriter(ctx, archiveRef)
	if err != nil {
		return nil, err
	}

	zw := zip.NewWriter(aw)
	registerZipCompressor(zw, opts.CompressionLevel)
	return &singleZipArchiveWriter{zw: zw, raw: aw}, nil
}

// newSplitZipArchiveWriter creates a rolling zip writer that emits `.partNNNN` volumes.
func (r *Runner) newSplitZipArchiveWriter(ctx context.Context, opts cli.Options, archiveRef locator.Ref) (zipArchiveWriter, error) {
	return &splitZipArchiveWriter{
		ctx:       ctx,
		runner:    r,
		baseRef:   archiveRef,
		level:     opts.CompressionLevel,
		splitSize: opts.SplitSizeBytes,
		nextPart:  1,
		partWidth: 4,
	}, nil
}

// registerZipCompressor applies the configured Deflate level to one zip writer.
func registerZipCompressor(zw *zip.Writer, level *int) {
	if level == nil {
		return
	}
	configuredLevel := *level
	zw.RegisterCompressor(zip.Deflate, func(dst io.Writer) (io.WriteCloser, error) {
		return flate.NewWriter(dst, configuredLevel)
	})
}

// singleZipArchiveWriter adapts the legacy zip writer to the zipArchiveWriter interface.
type singleZipArchiveWriter struct {
	zw  *zip.Writer
	raw io.WriteCloser
}

// CreateHeader writes one zip header to the underlying stream.
func (w *singleZipArchiveWriter) CreateHeader(hdr *zip.FileHeader) (io.Writer, error) {
	return w.zw.CreateHeader(hdr)
}

// FinishEntry is a no-op for single-stream archives.
func (w *singleZipArchiveWriter) FinishEntry() error {
	return nil
}

// Close finalizes the zip stream and then closes the output writer.
func (w *singleZipArchiveWriter) Close() error {
	var first error
	if err := w.zw.Close(); err != nil {
		first = fmt.Errorf("closing zip writer: %w", err)
	}
	if err := w.raw.Close(); err != nil && first == nil {
		first = fmt.Errorf("closing archive: %w", err)
	}
	return first
}

// splitZipArchiveWriter writes each volume as a complete zip archive.
type splitZipArchiveWriter struct {
	ctx       context.Context
	runner    *Runner
	baseRef   locator.Ref
	level     *int
	splitSize int64
	nextPart  int
	partWidth int

	current       *splitZipVolumeState
	rotateOnEntry bool
}

// splitZipVolumeState holds the writer stack for one active output volume.
type splitZipVolumeState struct {
	ref locator.Ref
	zw  *zip.Writer
	raw io.WriteCloser
	dst *countingWriteCloser
}

// CreateHeader opens or rotates volumes as needed and writes the next zip header.
func (w *splitZipArchiveWriter) CreateHeader(hdr *zip.FileHeader) (io.Writer, error) {
	if err := w.ensureVolume(); err != nil {
		return nil, err
	}
	return w.current.zw.CreateHeader(hdr)
}

// FinishEntry flushes the current zip volume and schedules rotation for the next member.
func (w *splitZipArchiveWriter) FinishEntry() error {
	if w.current == nil {
		return nil
	}
	if err := w.current.zw.Flush(); err != nil {
		return err
	}
	if shouldRotateSplitZipVolume(w.current.dst.count, w.splitSize) {
		w.rotateOnEntry = true
	}
	return nil
}

// shouldRotateSplitZipVolume decides whether the next member must start in a new volume.
//
// zip.Writer.Flush only drains the archive's outer bufio.Writer; it does not finalize
// the current member. Finalizing one zip entry appends a small deflate terminator
// plus a 16/24-byte data descriptor, so split ZIP mode keeps a small safety margin
// to avoid starting the next member on a volume that would exceed the threshold once
// the current member is fully finalized.
func shouldRotateSplitZipVolume(written int64, splitSize int64) bool {
	if splitSize <= 0 {
		return false
	}
	if splitSize <= splitZipEntryFinalizeLookaheadBytes {
		return true
	}
	return written >= splitSize-splitZipEntryFinalizeLookaheadBytes
}

// Close finalizes the active volume without creating a trailing empty volume.
func (w *splitZipArchiveWriter) Close() error {
	if w.current == nil {
		return nil
	}
	return w.closeCurrentVolume()
}

func (w *splitZipArchiveWriter) ensureVolume() error {
	if w.current == nil {
		return w.openNextVolume()
	}
	if !w.rotateOnEntry {
		return nil
	}
	if err := w.closeCurrentVolume(); err != nil {
		return err
	}
	return w.openNextVolume()
}

func (w *splitZipArchiveWriter) openNextVolume() error {
	ref := archiveSplitRef(w.baseRef, w.nextPart, w.partWidth)
	raw, err := w.runner.openArchiveWriter(w.ctx, ref)
	if err != nil {
		return err
	}
	dst := &countingWriteCloser{WriteCloser: raw}
	zw := zip.NewWriter(dst)
	registerZipCompressor(zw, w.level)

	w.current = &splitZipVolumeState{
		ref: ref,
		zw:  zw,
		raw: raw,
		dst: dst,
	}
	w.nextPart++
	w.rotateOnEntry = false
	return nil
}

func (w *splitZipArchiveWriter) closeCurrentVolume() error {
	if w.current == nil {
		return nil
	}

	var first error
	if err := w.current.zw.Close(); err != nil {
		first = fmt.Errorf("closing zip writer for %s: %w", w.current.ref.Raw, err)
	}
	if err := w.current.raw.Close(); err != nil && first == nil {
		first = fmt.Errorf("closing archive for %s: %w", w.current.ref.Raw, err)
	}
	w.current = nil
	w.rotateOnEntry = false
	return first
}
