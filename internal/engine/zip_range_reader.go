package engine

import (
	"context"
	"fmt"
	"io"
	"sync"
)

const defaultRemoteZipReadBlockSize int64 = 1 << 20

// zipRangeOpener opens one exact byte range from a remote archive source.
type zipRangeOpener func(ctx context.Context, offset int64, length int64) (io.ReadCloser, error)

// remoteZipReaderAt adapts byte-range requests into a cached io.ReaderAt.
type remoteZipReaderAt struct {
	ctx       context.Context
	size      int64
	blockSize int64
	openRange zipRangeOpener

	mu         sync.Mutex
	blockStart int64
	block      []byte
}

// newRemoteZipReaderAt builds an io.ReaderAt that serves remote zip reads from
// aligned range requests and caches the most recent block.
func newRemoteZipReaderAt(ctx context.Context, size int64, blockSize int64, openRange zipRangeOpener) io.ReaderAt {
	if blockSize <= 0 {
		blockSize = defaultRemoteZipReadBlockSize
	}
	return &remoteZipReaderAt{
		ctx:        ctx,
		size:       size,
		blockSize:  blockSize,
		openRange:  openRange,
		blockStart: -1,
	}
}

// ReadAt reads one exact byte region using cached range fetches.
func (r *remoteZipReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if off < 0 {
		return 0, fmt.Errorf("negative read offset %d", off)
	}
	if off >= r.size {
		return 0, io.EOF
	}

	limit := int64(len(p))
	if remaining := r.size - off; remaining < limit {
		limit = remaining
	}
	target := p[:limit]

	copied := 0
	for copied < len(target) {
		chunk, err := r.sliceAt(off+int64(copied), int64(len(target)-copied))
		if err != nil {
			if copied > 0 {
				return copied, err
			}
			return 0, err
		}
		n := copy(target[copied:], chunk)
		if n == 0 {
			if copied > 0 {
				return copied, io.ErrUnexpectedEOF
			}
			return 0, io.ErrUnexpectedEOF
		}
		copied += n
	}

	if limit < int64(len(p)) {
		return copied, io.EOF
	}
	return copied, nil
}

// sliceAt returns cached or freshly fetched bytes beginning at off.
func (r *remoteZipReaderAt) sliceAt(off int64, minLen int64) ([]byte, error) {
	r.mu.Lock()
	if off >= r.blockStart && off < r.blockStart+int64(len(r.block)) {
		start := off - r.blockStart
		chunk := r.block[start:]
		r.mu.Unlock()
		return chunk, nil
	}

	blockSize := r.blockSize
	fetchSize := max(blockSize, minLen)
	blockStart := off
	if fetchSize == blockSize {
		blockStart = off - (off % blockSize)
	}
	if remaining := r.size - blockStart; fetchSize > remaining {
		fetchSize = remaining
	}
	r.mu.Unlock()

	block, err := r.fetch(blockStart, fetchSize)
	if err != nil {
		return nil, err
	}

	r.mu.Lock()
	r.blockStart = blockStart
	r.block = block
	start := off - r.blockStart
	chunk := r.block[start:]
	r.mu.Unlock()
	return chunk, nil
}

// fetch loads one exact byte range into memory.
func (r *remoteZipReaderAt) fetch(offset int64, length int64) ([]byte, error) {
	rc, err := r.openRange(r.ctx, offset, length)
	if err != nil {
		return nil, err
	}
	defer rc.Close() //nolint:errcheck

	buf := make([]byte, length)
	if _, err := io.ReadFull(rc, buf); err != nil {
		return nil, err
	}
	return buf, nil
}
