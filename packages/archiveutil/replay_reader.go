package archiveutil

import (
	"bytes"
	"errors"
	"io"
	"sync"
)

// replayReadCloser replays prefetched bytes before reading from the source.
type replayReadCloser struct {
	reader io.Reader
	closer io.Closer
}

// closeOnceCloser guarantees that the wrapped closer is invoked at most once.
type closeOnceCloser struct {
	closer io.Closer
	once   sync.Once
	err    error
}

// Close closes the wrapped closer once and returns the same result thereafter.
func (c *closeOnceCloser) Close() error {
	if c == nil || c.closer == nil {
		return nil
	}
	c.once.Do(func() {
		c.err = c.closer.Close()
	})
	return c.err
}

// Read forwards reads to the replay reader.
func (r *replayReadCloser) Read(p []byte) (int, error) {
	return r.reader.Read(p)
}

// Close closes the wrapped source closer.
func (r *replayReadCloser) Close() error {
	return r.closer.Close()
}

// NewReplayReadCloser builds a reader that emits prefix first, then src.
//
// The returned closer is idempotent and closes src at most once.
func NewReplayReadCloser(prefix []byte, src io.ReadCloser) io.ReadCloser {
	closer := &closeOnceCloser{closer: src}
	if len(prefix) == 0 {
		return &replayReadCloser{reader: src, closer: closer}
	}
	prefetched := append([]byte(nil), prefix...)
	return &replayReadCloser{
		reader: io.MultiReader(bytes.NewReader(prefetched), src),
		closer: closer,
	}
}

// ReplayWithMagicPrefix reads up to prefixLen bytes and returns a reader that
// yields those bytes again before streaming the remaining source bytes.
func ReplayWithMagicPrefix(src io.ReadCloser, prefixLen int) ([]byte, io.ReadCloser, error) {
	if prefixLen <= 0 {
		return nil, NewReplayReadCloser(nil, src), nil
	}
	buf := make([]byte, prefixLen)
	n, err := io.ReadFull(src, buf)
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return nil, nil, err
	}
	prefix := append([]byte(nil), buf[:n]...)
	replay := NewReplayReadCloser(prefix, src)
	return prefix, replay, nil
}
