package engine

import (
	"context"
	"errors"
	"io"
)

const contextCopyBufferSize = 32 * 1024

// errInvalidWrite means that a write returned an impossible count.
var errInvalidWrite = errors.New("invalid write result")

// copyWithContext copies src into dst while checking ctx cancellation between
// read iterations so long-running staging copies can stop promptly.
func copyWithContext(ctx context.Context, dst io.Writer, src io.Reader) (int64, error) {
	size := contextCopyBufferSize
	if l, ok := src.(*io.LimitedReader); ok && int64(size) > l.N {
		if l.N < 1 {
			size = 1
		} else {
			size = int(l.N)
		}
	}
	buf := make([]byte, size)

	var written int64
	for {
		select {
		case <-ctx.Done():
			return written, ctx.Err()
		default:
		}

		nr, rerr := src.Read(buf)
		if nr > 0 {
			nw, werr := dst.Write(buf[:nr])
			if nw < 0 || nr < nw {
				nw = 0
				if werr == nil {
					werr = errInvalidWrite
				}
			}
			written += int64(nw)
			if werr != nil {
				return written, werr
			}
			if nw != nr {
				return written, io.ErrShortWrite
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
