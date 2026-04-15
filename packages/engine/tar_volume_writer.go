package engine

import (
	"archive/tar"
	"context"
	"fmt"
	"io"

	"github.com/islishude/gotgz/packages/cli"
	"github.com/islishude/gotgz/packages/compress"
	"github.com/islishude/gotgz/packages/locator"
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
