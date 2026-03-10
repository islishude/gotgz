package engine

import (
	"archive/tar"
	"context"
	"fmt"
	"io"

	"github.com/islishude/gotgz/internal/archivepath"
	"github.com/islishude/gotgz/internal/archiveutil"
	"github.com/islishude/gotgz/internal/cli"
	"github.com/islishude/gotgz/internal/compress"
	"github.com/islishude/gotgz/internal/locator"
)

// tarArchiveWriter writes tar entries and lets callers signal member boundaries.
type tarArchiveWriter interface {
	WriteHeader(hdr *tar.Header) error
	Write(p []byte) (int, error)
	FinishEntry() error
	Close() error
}

// newTarArchiveWriter returns either the legacy single-file writer or a split-volume writer.
func (r *Runner) newTarArchiveWriter(ctx context.Context, opts cli.Options, archiveRef locator.Ref) (tarArchiveWriter, error) {
	if opts.SplitSizeBytes <= 0 {
		return r.newSingleTarArchiveWriter(ctx, opts, archiveRef)
	}
	return r.newSplitTarArchiveWriter(ctx, opts, archiveRef)
}

// newSingleTarArchiveWriter creates the existing single-stream tar output pipeline.
func (r *Runner) newSingleTarArchiveWriter(ctx context.Context, opts cli.Options, archiveRef locator.Ref) (tarArchiveWriter, error) {
	aw, err := r.openArchiveWriter(ctx, archiveRef)
	if err != nil {
		return nil, err
	}

	cw, err := compress.NewWriter(aw, compress.FromString(string(opts.Compression)), compress.WriterOptions{Level: opts.CompressionLevel})
	if err != nil {
		if closeErr := aw.Close(); closeErr != nil {
			return nil, fmt.Errorf("create archive writer: %w (close: %v)", err, closeErr)
		}
		return nil, err
	}

	return &singleTarArchiveWriter{
		tw: tar.NewWriter(cw),
		cw: cw,
	}, nil
}

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

// singleTarArchiveWriter adapts the legacy tar writer to the tarArchiveWriter interface.
type singleTarArchiveWriter struct {
	tw *tar.Writer
	cw io.WriteCloser
}

// WriteHeader writes one tar header to the underlying stream.
func (w *singleTarArchiveWriter) WriteHeader(hdr *tar.Header) error {
	return w.tw.WriteHeader(hdr)
}

// Write forwards file data into the tar stream.
func (w *singleTarArchiveWriter) Write(p []byte) (int, error) {
	return w.tw.Write(p)
}

// FinishEntry is a no-op for single-stream archives.
func (w *singleTarArchiveWriter) FinishEntry() error {
	return nil
}

// Close finishes the tar stream and then closes the compression/output writer stack.
func (w *singleTarArchiveWriter) Close() error {
	var first error
	if err := w.tw.Close(); err != nil {
		first = fmt.Errorf("closing tar writer: %w", err)
	}
	if err := w.cw.Close(); err != nil && first == nil {
		first = fmt.Errorf("closing archive: %w", err)
	}
	return first
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

// Close finalizes the active volume without creating a trailing empty volume.
func (w *splitTarArchiveWriter) Close() error {
	if w.current == nil {
		return nil
	}
	return w.closeCurrentVolume()
}

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

// countingWriteCloser counts bytes written to the wrapped archive destination.
type countingWriteCloser struct {
	io.WriteCloser
	count int64
}

// Write forwards data and records how many bytes reached the destination.
func (w *countingWriteCloser) Write(p []byte) (int, error) {
	n, err := w.WriteCloser.Write(p)
	w.count += int64(n)
	return n, err
}

// archiveSplitRef builds the concrete path/key for one split output volume.
func archiveSplitRef(ref locator.Ref, part int, width int) locator.Ref {
	out := ref
	switch ref.Kind {
	case locator.KindLocal:
		out.Path = archivepath.FormatSplit(ref.Path, part, width)
		out.Raw = out.Path
	case locator.KindS3:
		out.Key = archivepath.FormatSplit(ref.Key, part, width)
		out.Raw = fmt.Sprintf("s3://%s/%s", out.Bucket, out.Key)
	}
	return out
}
