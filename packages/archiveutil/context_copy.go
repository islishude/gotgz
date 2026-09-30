package archiveutil

import (
	"context"
	"errors"
	"io"
)

const contextCopyBufferSize = 32 * 1024

// ErrInvalidWrite means that a write returned an impossible count.
var ErrInvalidWrite = errors.New("invalid write result")

// ErrCopyLimitExceeded means that a copy exceeded the configured byte limit.
var ErrCopyLimitExceeded = errors.New("copy limit exceeded")

// CopyWithContext copies src into dst while checking ctx cancellation between
// read iterations so long-running staging copies can stop promptly.
func CopyWithContext(ctx context.Context, dst io.Writer, src io.Reader) (int64, error) {
	return CopyWithContextLimit(ctx, dst, src, -1)
}

// CopyWithContextLimit copies src into dst while enforcing a hard byte limit.
// A negative limit disables the bound.
func CopyWithContextLimit(ctx context.Context, dst io.Writer, src io.Reader, limit int64) (int64, error) {
	// Keep the buffer small for already bounded readers.
	size := contextCopyBufferSize
	if l, ok := src.(*io.LimitedReader); ok && int64(size) > l.N {
		size = max(int(l.N), 1)
	}

	buf := make([]byte, size)
	enforceLimit := limit >= 0

	var written int64
	for {
		select {
		case <-ctx.Done():
			return written, ctx.Err()
		default:
		}

		remaining := int64(0)
		readBuf := buf
		if enforceLimit {
			remaining = limit - written
			if remaining < int64(len(readBuf)) {
				readSize := max(remaining+1, 1)
				readBuf = buf[:readSize]
			}
		}

		nr, rerr := src.Read(readBuf)
		if nr > 0 {
			maxWrite := int64(-1)
			if enforceLimit {
				maxWrite = remaining
			}
			n, err := writeCopyChunk(dst, readBuf[:nr], maxWrite)
			written += int64(n)
			if err != nil {
				return written, err
			}
		}
		if rerr != nil {
			if errors.Is(rerr, io.EOF) {
				return written, nil
			}
			return written, rerr
		}
	}
}

// writeCopyChunk writes at most remaining bytes, preserving writer failures
// ahead of limit errors. A negative remaining value means unbounded.
func writeCopyChunk(dst io.Writer, data []byte, remaining int64) (int, error) {
	overflow := remaining >= 0 && int64(len(data)) > remaining
	if overflow {
		data = data[:remaining]
	}
	// A one-byte read after the limit is reached only probes for overflow.
	if len(data) == 0 {
		return 0, ErrCopyLimitExceeded
	}
	n, err := dst.Write(data)
	if n < 0 || n > len(data) {
		n = 0
		if err == nil {
			err = ErrInvalidWrite
		}
	}
	if err != nil {
		return n, err
	}
	if n != len(data) {
		return n, io.ErrShortWrite
	}
	if overflow {
		return n, ErrCopyLimitExceeded
	}
	return n, nil
}
