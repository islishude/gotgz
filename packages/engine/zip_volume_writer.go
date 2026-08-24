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

// zipArchiveWriter writes zip entries and lets callers rotate only at member boundaries.
type zipArchiveWriter interface {
	CreateHeader(hdr *zip.FileHeader) (io.Writer, error)
	FinishEntry() error
	Close() error
	Abort(error) error
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
	session, err := r.beginArchiveWriter(ctx, archiveRef)
	if err != nil {
		return nil, err
	}

	zw := zip.NewWriter(nonClosingWriteCloser{Writer: session})
	registerZipCompressor(zw, opts.CompressionLevel)
	return &singleZipArchiveWriter{zw: zw, session: session}, nil
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
	zw      *zip.Writer
	session archiveWriteSession
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
	if err := w.zw.Close(); err != nil {
		_ = w.session.Abort(err)
		return fmt.Errorf("closing zip writer: %w", err)
	}
	if err := w.session.Commit(); err != nil {
		_ = w.session.Abort(err)
		return fmt.Errorf("committing archive: %w", err)
	}
	return nil
}

func (w *singleZipArchiveWriter) Abort(cause error) error {
	abortErr := w.session.Abort(cause)
	_ = w.zw.Close()
	return abortErr
}
