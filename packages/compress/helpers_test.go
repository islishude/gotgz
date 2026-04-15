package compress

import (
	"bytes"
	"io"
)

// nopWriteCloser wraps a Writer with a no-op Close.
type nopWriteCloser struct{ io.Writer }

// Close is a no-op.
func (n nopWriteCloser) Close() error { return nil }

// errReadCloser is a ReadCloser that always returns the configured error on Read.
type errReadCloser struct {
	err error
}

// Read always returns the configured error.
func (r *errReadCloser) Read([]byte) (int, error) { return 0, r.err }

// Close is a no-op.
func (r *errReadCloser) Close() error { return nil }

// closeRecorder records close order and can inject a close error.
type closeRecorder struct {
	name     string
	events   *[]string
	closeErr error
}

// Close records the close operation and returns the configured error.
func (r *closeRecorder) Close() error {
	*r.events = append(*r.events, "close:"+r.name)
	return r.closeErr
}

// writeCloserRecorder records writes, flushes, and closes for stacked writer tests.
type writeCloserRecorder struct {
	name     string
	events   *[]string
	buf      bytes.Buffer
	writeErr error
	closeErr error
	flushErr error
}

// Write records the write operation and appends the payload to the internal buffer.
func (r *writeCloserRecorder) Write(p []byte) (int, error) {
	*r.events = append(*r.events, "write:"+r.name)
	if r.writeErr != nil {
		return 0, r.writeErr
	}
	return r.buf.Write(p)
}

// Close records the close operation and returns the configured error.
func (r *writeCloserRecorder) Close() error {
	*r.events = append(*r.events, "close:"+r.name)
	return r.closeErr
}

// Flush records the flush operation and returns the configured error.
func (r *writeCloserRecorder) Flush() error {
	*r.events = append(*r.events, "flush:"+r.name)
	return r.flushErr
}
