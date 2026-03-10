package engine

import (
	"context"
	"errors"
	"io"
)

const contextCopyBufferSize = 32 * 1024

// errInvalidWrite means that a write returned an impossible count.
var errInvalidWrite = errors.New("invalid write result")

// errCopyLimitExceeded means that a copy exceeded the configured byte limit.
var errCopyLimitExceeded = errors.New("copy limit exceeded")

// copyWithContext copies src into dst while checking ctx cancellation between
// read iterations so long-running staging copies can stop promptly.
func copyWithContext(ctx context.Context, dst io.Writer, src io.Reader) (int64, error) {
	return copyWithContextLimit(ctx, dst, src, -1)
}

// copyWithContextLimit copies src into dst while enforcing a hard byte limit.
// A negative limit disables the bound.
func copyWithContextLimit(ctx context.Context, dst io.Writer, src io.Reader, limit int64) (int64, error) {
	// copies and shrinks it for small limited readers.
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
			writeCount := nr
			if enforceLimit && int64(writeCount) > remaining {
				writeCount = int(remaining)
			}

			// When remaining is 0 the read was a 1-byte probe to detect
			// overflow; skip the write and report the limit breach.
			if enforceLimit && writeCount == 0 {
				return written, errCopyLimitExceeded
			}

			p := readBuf[:writeCount]
			nw, werr := dst.Write(p)
			if nw < 0 || len(p) < nw {
				nw = 0
				if werr == nil {
					werr = errInvalidWrite
				}
			}
			written += int64(nw)
			if werr != nil {
				return written, werr
			}
			if nw != len(p) {
				return written, io.ErrShortWrite
			}
			if enforceLimit && int64(nr) > remaining {
				return written, errCopyLimitExceeded
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
