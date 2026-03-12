package archiveprogress

import "io"

// CountingReader reports bytes read to a progress reporter.
type CountingReader struct {
	reader   io.Reader
	reporter *Reporter
}

// NewCountingReader wraps a reader and records all successful reads.
func NewCountingReader(reader io.Reader, reporter *Reporter) io.Reader {
	if reporter == nil || !reporter.enabled {
		return reader
	}
	return &CountingReader{reader: reader, reporter: reporter}
}

// Read reads from the underlying reader and records progress.
func (r *CountingReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if n > 0 {
		r.reporter.AddDone(int64(n))
	}
	return n, err
}

// CountingReadCloser wraps an io.ReadCloser and reports bytes read to a
// progress reporter. It embeds countingReader to avoid duplicating the Read
// implementation.
type CountingReadCloser struct {
	CountingReader
	closer io.Closer
}

// NewCountingReadCloser wraps a read closer and records all successful reads.
func NewCountingReadCloser(reader io.ReadCloser, reporter *Reporter) io.ReadCloser {
	if reporter == nil || !reporter.enabled {
		return reader
	}
	return &CountingReadCloser{
		CountingReader: CountingReader{reader: reader, reporter: reporter},
		closer:         reader,
	}
}

// Close closes the wrapped reader.
func (r *CountingReadCloser) Close() error {
	return r.closer.Close()
}
