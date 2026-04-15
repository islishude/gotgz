package engine

import (
	"archive/tar"
	"context"
	"fmt"
	"io"

	"github.com/islishude/gotgz/packages/archiveutil"
	"github.com/islishude/gotgz/packages/cli"
	"github.com/islishude/gotgz/packages/compress"
	"github.com/islishude/gotgz/packages/locator"
)

const splitFlushLookaheadBytes int64 = 1 << 20

// newSplitTarArchiveWriter creates a rolling tar writer that emits `.partNNNN` volumes.
func (r *Runner) newSplitTarArchiveWriter(ctx context.Context, opts cli.Options, archiveRef locator.Ref) (tarArchiveWriter, error) {
	return &splitTarArchiveWriter{
		ctx:         ctx,
		runner:      r,
		baseRef:     archiveRef,
		compression: compress.FromString(string(opts.Compression)),
		level:       opts.CompressionLevel,
		splitSize:   opts.SplitSizeBytes,
		nextPart:    1,
		partWidth:   4,
	}, nil
}

// splitTarArchiveWriter writes each volume as a complete tar archive.
type splitTarArchiveWriter struct {
	ctx         context.Context
	runner      *Runner
	baseRef     locator.Ref
	compression compress.Type
	level       *int
	splitSize   int64
	nextPart    int
	partWidth   int

	current       *splitTarVolumeState
	rotateOnEntry bool
}

// splitTarVolumeState holds the writer stack for one active output volume.
type splitTarVolumeState struct {
	ref locator.Ref
	tw  *tar.Writer
	cw  io.WriteCloser
	raw io.WriteCloser
	dst *countingWriteCloser
}

// WriteHeader opens or rotates volumes as needed and writes the next tar header.
func (w *splitTarArchiveWriter) WriteHeader(hdr *tar.Header) error {
	if err := w.ensureVolume(); err != nil {
		return err
	}
	return w.current.tw.WriteHeader(hdr)
}

// Write forwards file data into the current volume tar stream.
func (w *splitTarArchiveWriter) Write(p []byte) (int, error) {
	if err := w.ensureVolume(); err != nil {
		return 0, err
	}
	return w.current.tw.Write(p)
}

// FinishEntry flushes tar/compression buffers and schedules rotation for the next member.
func (w *splitTarArchiveWriter) FinishEntry() error {
	if w.current == nil {
		return nil
	}
	if !shouldFlushSplitVolume(w.current.dst.count, w.splitSize) {
		return nil
	}
	if err := w.current.tw.Flush(); err != nil {
		return err
	}
	if flusher, ok := w.current.cw.(compress.FlushWriteCloser); ok {
		if err := flusher.Flush(); err != nil {
			return err
		}
	}
	if w.current.dst.count >= w.splitSize {
		w.rotateOnEntry = true
	}
	return nil
}

// shouldFlushSplitVolume reports whether the current volume is close enough to
// the split threshold that flushing buffered output is worth the cost.
func shouldFlushSplitVolume(written int64, splitSize int64) bool {
	if splitSize <= 0 || splitSize <= splitFlushLookaheadBytes {
		return true
	}
	return written >= splitSize-splitFlushLookaheadBytes
}

// Close finalizes the active volume without creating a trailing empty volume.
func (w *splitTarArchiveWriter) Close() error {
	if w.current == nil {
		return nil
	}
	return w.closeCurrentVolume()
}

// ensureVolume opens the first output volume or rotates to the next one when requested.
func (w *splitTarArchiveWriter) ensureVolume() error {
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

// openNextVolume creates the next split volume writer stack and makes it current.
func (w *splitTarArchiveWriter) openNextVolume() error {
	ref := archiveSplitRef(w.baseRef, w.nextPart, w.partWidth)
	raw, err := w.runner.openArchiveWriter(w.ctx, ref)
	if err != nil {
		return err
	}
	dst := &countingWriteCloser{WriteCloser: raw}
	cw, err := compress.NewWriter(dst, w.compression, compress.WriterOptions{Level: w.level})
	if err != nil {
		if closeErr := raw.Close(); closeErr != nil {
			return fmt.Errorf("create archive writer: %w (close: %v)", err, closeErr)
		}
		return err
	}

	w.current = &splitTarVolumeState{
		ref: ref,
		tw:  tar.NewWriter(cw),
		cw:  cw,
		raw: raw,
		dst: dst,
	}
	w.nextPart++
	w.rotateOnEntry = false
	return nil
}

// closeCurrentVolume finalizes the active split volume and clears current state.
func (w *splitTarArchiveWriter) closeCurrentVolume() error {
	if w.current == nil {
		return nil
	}

	var first error
	if err := w.current.tw.Close(); err != nil {
		first = fmt.Errorf("closing tar writer for %s: %w", archiveutil.NameHint(w.current.ref), err)
	}
	if err := w.current.cw.Close(); err != nil && first == nil {
		first = fmt.Errorf("closing archive for %s: %w", archiveutil.NameHint(w.current.ref), err)
	}
	w.current = nil
	w.rotateOnEntry = false
	return first
}
