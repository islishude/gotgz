package engine

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"
)

// TestCopyWithContext_Basic verifies that a simple copy transfers all bytes and
// reports the correct written count.
func TestCopyWithContext_Basic(t *testing.T) {
	src := strings.NewReader("hello world")
	var dst bytes.Buffer

	n, err := copyWithContext(context.Background(), &dst, src)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != int64(len("hello world")) {
		t.Fatalf("expected %d bytes written, got %d", len("hello world"), n)
	}
	if dst.String() != "hello world" {
		t.Fatalf("expected %q, got %q", "hello world", dst.String())
	}
}

// TestCopyWithContext_EmptySource ensures copying from an empty reader
// succeeds with zero bytes written.
func TestCopyWithContext_EmptySource(t *testing.T) {
	src := strings.NewReader("")
	var dst bytes.Buffer

	n, err := copyWithContext(context.Background(), &dst, src)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 0 {
		t.Fatalf("expected 0 bytes written, got %d", n)
	}
}

// TestCopyWithContext_LargePayload copies more data than the internal buffer
// size (32 KB) to exercise multiple read iterations.
func TestCopyWithContext_LargePayload(t *testing.T) {
	// 128 KB payload — forces at least 4 iterations with the 32 KB buffer.
	data := bytes.Repeat([]byte("A"), 128*1024)
	src := bytes.NewReader(data)
	var dst bytes.Buffer

	n, err := copyWithContext(context.Background(), &dst, src)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != int64(len(data)) {
		t.Fatalf("expected %d bytes written, got %d", len(data), n)
	}
	if !bytes.Equal(dst.Bytes(), data) {
		t.Fatal("copied data does not match source")
	}
}

// TestCopyWithContext_AlreadyCancelled confirms that a pre-cancelled context
// returns an error immediately without copying any data.
func TestCopyWithContext_AlreadyCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	src := strings.NewReader("should not be copied")
	var dst bytes.Buffer

	n, err := copyWithContext(ctx, &dst, src)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if n != 0 {
		t.Fatalf("expected 0 bytes written, got %d", n)
	}
}

// TestCopyWithContext_CancelledDuringCopy ensures cancellation during an
// ongoing copy stops the operation and returns a context error.
func TestCopyWithContext_CancelledDuringCopy(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	// slowReader cancels the context after the first read so the next
	// iteration detects cancellation.
	sr := &slowReader{
		data:       bytes.Repeat([]byte("X"), 128*1024),
		chunkSize:  1024,
		afterFirst: cancel,
	}

	var dst bytes.Buffer
	_, err := copyWithContext(ctx, &dst, sr)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

// TestCopyWithContext_ReadError propagates a read error that is not io.EOF.
func TestCopyWithContext_ReadError(t *testing.T) {
	readErr := errors.New("disk read failure")
	src := &errReader{err: readErr}
	var dst bytes.Buffer

	_, err := copyWithContext(context.Background(), &dst, src)
	if !errors.Is(err, readErr) {
		t.Fatalf("expected %v, got %v", readErr, err)
	}
}

// TestCopyWithContext_WriteError propagates a write error from the destination.
func TestCopyWithContext_WriteError(t *testing.T) {
	writeErr := errors.New("disk full")
	src := strings.NewReader("some data to write")
	dst := &errWriter{err: writeErr}

	_, err := copyWithContext(context.Background(), dst, src)
	if !errors.Is(err, writeErr) {
		t.Fatalf("expected %v, got %v", writeErr, err)
	}
}

// TestCopyWithContext_ShortWrite detects when the writer accepts fewer bytes
// than provided and returns io.ErrShortWrite.
func TestCopyWithContext_ShortWrite(t *testing.T) {
	src := strings.NewReader("hello world")
	dst := &shortWriter{}

	_, err := copyWithContext(context.Background(), dst, src)
	if !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("expected io.ErrShortWrite, got %v", err)
	}
}

// TestCopyWithContext_NegativeWriteCount ensures that a writer returning a
// negative byte count is treated as an invalid write.
func TestCopyWithContext_NegativeWriteCount(t *testing.T) {
	src := strings.NewReader("some data")
	dst := &negativeWriter{}

	n, err := copyWithContext(context.Background(), dst, src)
	if !errors.Is(err, errInvalidWrite) {
		t.Fatalf("expected errInvalidWrite, got %v", err)
	}
	if n != 0 {
		t.Fatalf("expected 0 bytes written, got %d", n)
	}
}

// TestCopyWithContext_OverreportedWriteCount ensures that a writer claiming to
// have written more bytes than it received is treated as an invalid write.
func TestCopyWithContext_OverreportedWriteCount(t *testing.T) {
	src := strings.NewReader("data")
	dst := &overreportWriter{}

	n, err := copyWithContext(context.Background(), dst, src)
	if !errors.Is(err, errInvalidWrite) {
		t.Fatalf("expected errInvalidWrite, got %v", err)
	}
	if n != 0 {
		t.Fatalf("expected 0 bytes written, got %d", n)
	}
}

// TestCopyWithContext_DeadlineExceeded ensures context.DeadlineExceeded is
// returned when a deadline expires.
func TestCopyWithContext_DeadlineExceeded(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()

	// Give the deadline time to expire before the copy starts.
	time.Sleep(5 * time.Millisecond)

	src := strings.NewReader("data")
	var dst bytes.Buffer

	_, err := copyWithContext(ctx, &dst, src)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}
}

// TestCopyWithContext_ReadErrorWithPartialData ensures that bytes written before
// a read error are still tracked in the returned count.
func TestCopyWithContext_ReadErrorWithPartialData(t *testing.T) {
	readErr := errors.New("connection reset")
	// partialErrReader returns some data on the first read and an error on the
	// second read.
	src := &partialErrReader{
		data: []byte("partial"),
		err:  readErr,
	}
	var dst bytes.Buffer

	n, err := copyWithContext(context.Background(), &dst, src)
	if !errors.Is(err, readErr) {
		t.Fatalf("expected %v, got %v", readErr, err)
	}
	if n != int64(len("partial")) {
		t.Fatalf("expected %d bytes written, got %d", len("partial"), n)
	}
}

// TestCopyWithContext_LimitedReaderSmallLimit verifies that when the source is
// an io.LimitedReader with N smaller than the default buffer size, the buffer
// is sized to N instead.
func TestCopyWithContext_LimitedReaderSmallLimit(t *testing.T) {
	data := []byte("limited payload")
	inner := bytes.NewReader(data)
	src := &io.LimitedReader{R: inner, N: int64(len(data))}
	var dst bytes.Buffer

	n, err := copyWithContext(context.Background(), &dst, src)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != int64(len(data)) {
		t.Fatalf("expected %d bytes written, got %d", len(data), n)
	}
	if !bytes.Equal(dst.Bytes(), data) {
		t.Fatal("copied data does not match source")
	}
}

// TestCopyWithContext_LimitedReaderZeroLimit verifies that when the source is
// an io.LimitedReader with N <= 0, the buffer is clamped to size 1 and the
// copy still completes (immediately hitting EOF from the LimitedReader).
func TestCopyWithContext_LimitedReaderZeroLimit(t *testing.T) {
	inner := strings.NewReader("anything")
	src := &io.LimitedReader{R: inner, N: 0}
	var dst bytes.Buffer

	n, err := copyWithContext(context.Background(), &dst, src)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 0 {
		t.Fatalf("expected 0 bytes written, got %d", n)
	}
}

// TestCopyWithContext_LimitedReaderNegativeLimit verifies that when the source
// is an io.LimitedReader with a negative N, the buffer is clamped to size 1.
func TestCopyWithContext_LimitedReaderNegativeLimit(t *testing.T) {
	inner := strings.NewReader("anything")
	src := &io.LimitedReader{R: inner, N: -5}
	var dst bytes.Buffer

	n, err := copyWithContext(context.Background(), &dst, src)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 0 {
		t.Fatalf("expected 0 bytes written, got %d", n)
	}
}

// TestCopyWithContext_LimitedReaderLargeLimit verifies that when the source is
// an io.LimitedReader with N larger than the default buffer, the default buffer
// size is used and the data is copied correctly.
func TestCopyWithContext_LimitedReaderLargeLimit(t *testing.T) {
	data := bytes.Repeat([]byte("B"), 64*1024)
	inner := bytes.NewReader(data)
	src := &io.LimitedReader{R: inner, N: int64(len(data))}
	var dst bytes.Buffer

	n, err := copyWithContext(context.Background(), &dst, src)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != int64(len(data)) {
		t.Fatalf("expected %d bytes written, got %d", len(data), n)
	}
	if !bytes.Equal(dst.Bytes(), data) {
		t.Fatal("copied data does not match source")
	}
}

// TestCopyWithContextLimit_WithinLimit ensures the bounded copy succeeds when
// the source fits under the configured limit.
func TestCopyWithContextLimit_WithinLimit(t *testing.T) {
	src := strings.NewReader("hello")
	var dst bytes.Buffer

	n, err := copyWithContextLimit(context.Background(), &dst, src, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 5 {
		t.Fatalf("expected 5 bytes written, got %d", n)
	}
	if dst.String() != "hello" {
		t.Fatalf("expected %q, got %q", "hello", dst.String())
	}
}

// TestCopyWithContextLimit_ExactLimit ensures hitting the limit exactly does
// not report an overflow.
func TestCopyWithContextLimit_ExactLimit(t *testing.T) {
	data := []byte("exact")
	src := bytes.NewReader(data)
	var dst bytes.Buffer

	n, err := copyWithContextLimit(context.Background(), &dst, src, int64(len(data)))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != int64(len(data)) {
		t.Fatalf("expected %d bytes written, got %d", len(data), n)
	}
	if !bytes.Equal(dst.Bytes(), data) {
		t.Fatal("copied data does not match source")
	}
}

// TestCopyWithContextLimit_Exceeded ensures the bounded copy writes only the
// allowed bytes before reporting the staging limit error.
func TestCopyWithContextLimit_Exceeded(t *testing.T) {
	src := strings.NewReader("overflow")
	var dst bytes.Buffer

	n, err := copyWithContextLimit(context.Background(), &dst, src, 5)
	if !errors.Is(err, errCopyLimitExceeded) {
		t.Fatalf("expected %v, got %v", errCopyLimitExceeded, err)
	}
	if n != 5 {
		t.Fatalf("expected 5 bytes written, got %d", n)
	}
	if dst.String() != "overf" {
		t.Fatalf("expected %q, got %q", "overf", dst.String())
	}
}

// TestCopyWithContextLimit_MinimalOverread ensures the bounded copy reads at
// most one byte past the limit when detecting an overflow.
func TestCopyWithContextLimit_MinimalOverread(t *testing.T) {
	src := &trackingReader{data: bytes.Repeat([]byte("Z"), 1024)}
	var dst bytes.Buffer

	n, err := copyWithContextLimit(context.Background(), &dst, src, 7)
	if !errors.Is(err, errCopyLimitExceeded) {
		t.Fatalf("expected %v, got %v", errCopyLimitExceeded, err)
	}
	if n != 7 {
		t.Fatalf("expected 7 bytes written, got %d", n)
	}
	if src.totalRead != 8 {
		t.Fatalf("expected 8 bytes read, got %d", src.totalRead)
	}
}

// TestCopyWithContextLimit_ZeroLimit ensures a zero-byte limit rejects any
// non-empty source without writing data while still probing only one byte.
func TestCopyWithContextLimit_ZeroLimit(t *testing.T) {
	src := &trackingReader{data: []byte("blocked")}
	var dst bytes.Buffer

	n, err := copyWithContextLimit(context.Background(), &dst, src, 0)
	if !errors.Is(err, errCopyLimitExceeded) {
		t.Fatalf("expected %v, got %v", errCopyLimitExceeded, err)
	}
	if n != 0 {
		t.Fatalf("expected 0 bytes written, got %d", n)
	}
	if dst.Len() != 0 {
		t.Fatalf("expected destination to stay empty, got %q", dst.String())
	}
	if src.totalRead != 1 {
		t.Fatalf("expected a 1-byte overflow probe, got %d bytes read", src.totalRead)
	}
}

// TestCopyWithContextLimit_NegativeLimitDisablesBound ensures copyWithContext's
// negative sentinel continues to behave as an unbounded copy.
func TestCopyWithContextLimit_NegativeLimitDisablesBound(t *testing.T) {
	src := strings.NewReader("unbounded")
	var dst bytes.Buffer

	n, err := copyWithContextLimit(context.Background(), &dst, src, -1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != int64(len("unbounded")) {
		t.Fatalf("expected %d bytes written, got %d", len("unbounded"), n)
	}
	if dst.String() != "unbounded" {
		t.Fatalf("expected %q, got %q", "unbounded", dst.String())
	}
}

// --- test helpers ---

// slowReader returns data in small chunks and calls afterFirst after delivering
// the first chunk.
type slowReader struct {
	data       []byte
	offset     int
	chunkSize  int
	afterFirst func()
	called     bool
}

// Read implements io.Reader for slowReader.
func (r *slowReader) Read(p []byte) (int, error) {
	if r.offset >= len(r.data) {
		return 0, io.EOF
	}
	end := min(r.offset+r.chunkSize, len(r.data))
	n := copy(p, r.data[r.offset:end])
	r.offset += n

	if !r.called && r.afterFirst != nil {
		r.afterFirst()
		r.called = true
	}
	return n, nil
}

// errReader always returns the configured error.
type errReader struct {
	err error
}

// Read implements io.Reader for errReader.
func (r *errReader) Read(_ []byte) (int, error) {
	return 0, r.err
}

// errWriter always returns the configured error.
type errWriter struct {
	err error
}

// Write implements io.Writer for errWriter.
func (w *errWriter) Write(_ []byte) (int, error) {
	return 0, w.err
}

// shortWriter always writes fewer bytes than requested.
type shortWriter struct{}

// Write implements io.Writer for shortWriter. It reports writing only one byte
// less than the slice length, triggering io.ErrShortWrite.
func (w *shortWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	return len(p) - 1, nil
}

// partialErrReader returns data on the first read and an error on subsequent
// reads.
type partialErrReader struct {
	data    []byte
	err     error
	readOne bool
}

// Read implements io.Reader for partialErrReader.
func (r *partialErrReader) Read(p []byte) (int, error) {
	if r.readOne {
		return 0, r.err
	}
	r.readOne = true
	n := copy(p, r.data)
	return n, nil
}

// negativeWriter returns a negative byte count from Write.
type negativeWriter struct{}

// Write implements io.Writer for negativeWriter.
func (w *negativeWriter) Write(_ []byte) (int, error) {
	return -1, nil
}

// overreportWriter claims to have written more bytes than it received.
type overreportWriter struct{}

// Write implements io.Writer for overreportWriter.
func (w *overreportWriter) Write(p []byte) (int, error) {
	return len(p) + 1, nil
}

// trackingReader records how many bytes the copy loop pulled from the source.
type trackingReader struct {
	data      []byte
	offset    int
	totalRead int
}

// Read implements io.Reader for trackingReader.
func (r *trackingReader) Read(p []byte) (int, error) {
	if r.offset >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.offset:])
	r.offset += n
	r.totalRead += n
	return n, nil
}
