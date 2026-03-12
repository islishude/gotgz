package archiveutil

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

// TestNewReplayReadCloser_ReplaysPrefix verifies that prefetched bytes are
// replayed before the underlying stream bytes.
func TestNewReplayReadCloser_ReplaysPrefix(t *testing.T) {
	src := io.NopCloser(bytes.NewReader([]byte("tail")))
	r := NewReplayReadCloser([]byte("head-"), src)

	data, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if string(data) != "head-tail" {
		t.Fatalf("ReadAll() = %q, want %q", string(data), "head-tail")
	}
}

// TestNewReplayReadCloser_CloseOnce verifies that close is idempotent and
// returns the same error value across repeated calls.
func TestNewReplayReadCloser_CloseOnce(t *testing.T) {
	closeErr := errors.New("close failed")
	src := &recordingReadCloser{closeErr: closeErr}
	r := NewReplayReadCloser(nil, src)

	err1 := r.Close()
	err2 := r.Close()
	if !errors.Is(err1, closeErr) {
		t.Fatalf("first Close() error = %v, want %v", err1, closeErr)
	}
	if !errors.Is(err2, closeErr) {
		t.Fatalf("second Close() error = %v, want %v", err2, closeErr)
	}
	if src.closeCalls != 1 {
		t.Fatalf("close call count = %d, want 1", src.closeCalls)
	}
}

// TestReplayWithMagicPrefix_ReplaysReadPrefix verifies that the helper reads
// and replays the requested prefix length.
func TestReplayWithMagicPrefix_ReplaysReadPrefix(t *testing.T) {
	src := io.NopCloser(bytes.NewReader([]byte("abcdef")))
	prefix, replay, err := ReplayWithMagicPrefix(src, 4)
	if err != nil {
		t.Fatalf("ReplayWithMagicPrefix() error = %v", err)
	}
	if string(prefix) != "abcd" {
		t.Fatalf("prefix = %q, want %q", string(prefix), "abcd")
	}
	data, err := io.ReadAll(replay)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if string(data) != "abcdef" {
		t.Fatalf("ReadAll() = %q, want %q", string(data), "abcdef")
	}
}

// TestReplayWithMagicPrefix_PrefixLongerThanStream verifies short streams are
// handled without error and still replay all consumed bytes.
func TestReplayWithMagicPrefix_PrefixLongerThanStream(t *testing.T) {
	src := io.NopCloser(bytes.NewReader([]byte("abc")))
	prefix, replay, err := ReplayWithMagicPrefix(src, 8)
	if err != nil {
		t.Fatalf("ReplayWithMagicPrefix() error = %v", err)
	}
	if string(prefix) != "abc" {
		t.Fatalf("prefix = %q, want %q", string(prefix), "abc")
	}
	data, err := io.ReadAll(replay)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if string(data) != "abc" {
		t.Fatalf("ReadAll() = %q, want %q", string(data), "abc")
	}
}

// TestReplayWithMagicPrefix_ReadError verifies non-EOF read errors are
// returned from ReplayWithMagicPrefix.
func TestReplayWithMagicPrefix_ReadError(t *testing.T) {
	readErr := errors.New("read failed")
	src := &prefixErrorReadCloser{err: readErr}
	_, _, err := ReplayWithMagicPrefix(src, 8)
	if !errors.Is(err, readErr) {
		t.Fatalf("ReplayWithMagicPrefix() error = %v, want %v", err, readErr)
	}
}

type recordingReadCloser struct {
	closeCalls int
	closeErr   error
}

func (r *recordingReadCloser) Read(_ []byte) (int, error) {
	return 0, io.EOF
}

func (r *recordingReadCloser) Close() error {
	r.closeCalls++
	return r.closeErr
}

type prefixErrorReadCloser struct {
	err error
}

func (r *prefixErrorReadCloser) Read(_ []byte) (int, error) {
	return 0, r.err
}

func (r *prefixErrorReadCloser) Close() error {
	return nil
}
