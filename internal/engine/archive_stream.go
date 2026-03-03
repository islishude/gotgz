package engine

import (
	"bytes"
	"errors"
	"io"
)

// replayReadCloser replays prefetched bytes before reading from the source.
type replayReadCloser struct {
	reader io.Reader
	closer io.Closer
}

// Read forwards reads to the replay reader.
func (r *replayReadCloser) Read(p []byte) (int, error) {
	return r.reader.Read(p)
}

// Close closes the wrapped source closer.
func (r *replayReadCloser) Close() error {
	return r.closer.Close()
}

// replayWithMagicPrefix reads up to prefixLen bytes and returns a reader that
// yields those bytes again before streaming the remaining source bytes.
func replayWithMagicPrefix(src io.ReadCloser, prefixLen int) ([]byte, io.ReadCloser, error) {
	if prefixLen <= 0 {
		return nil, src, nil
	}
	buf := make([]byte, prefixLen)
	n, err := io.ReadFull(src, buf)
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return nil, nil, err
	}
	prefix := append([]byte(nil), buf[:n]...)
	replay := &replayReadCloser{
		reader: io.MultiReader(bytes.NewReader(prefix), src),
		closer: src,
	}
	return prefix, replay, nil
}
