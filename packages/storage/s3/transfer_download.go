package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

var errTransferReaderClosed = errors.New("s3 transfer reader is closed")

func (m *transferManager) openReader(ctx context.Context, bucket, key string) (io.ReadCloser, Metadata, error) {
	if m == nil || m.client == nil {
		return nil, Metadata{}, fmt.Errorf("s3 transfer manager is not configured")
	}
	head, err := m.client.HeadObject(ctx, &awss3.HeadObjectInput{
		Bucket: new(bucket),
		Key:    new(key),
	})
	if err != nil {
		return nil, Metadata{}, err
	}
	if head == nil || head.ContentLength == nil {
		return nil, Metadata{}, fmt.Errorf("S3 HeadObject response omitted Content-Length")
	}
	size := *head.ContentLength
	if size < 0 {
		return nil, Metadata{}, fmt.Errorf("S3 HeadObject returned negative Content-Length %d", size)
	}
	if size > m.options.maxObjectSize {
		return nil, Metadata{}, fmt.Errorf("S3 object size %d exceeds supported limit %d", size, m.options.maxObjectSize)
	}
	metadata := Metadata{
		Size:         size,
		ContentType:  aws.ToString(head.ContentType),
		ETag:         aws.ToString(head.ETag),
		VersionID:    aws.ToString(head.VersionId),
		LastModified: aws.ToTime(head.LastModified),
	}
	if err := ctx.Err(); err != nil {
		return nil, Metadata{}, err
	}
	if size == 0 {
		return io.NopCloser(bytes.NewReader(nil)), metadata, nil
	}
	if strings.TrimSpace(metadata.VersionID) == "" && strings.TrimSpace(metadata.ETag) == "" {
		return nil, Metadata{}, fmt.Errorf("S3 HeadObject response omitted both VersionId and ETag")
	}

	reader, err := newTransferReader(ctx, m, bucket, key, metadata)
	if err != nil {
		return nil, Metadata{}, err
	}
	return reader, metadata, nil
}

type downloadPart struct {
	index int
	start int64
	end   int64
}

type downloadResult struct {
	index int
	body  []byte
}

type transferReader struct {
	ctx    context.Context
	cancel context.CancelCauseFunc

	manager *transferManager
	bucket  string
	key     string
	size    int64
	etag    string
	version string

	jobs    chan downloadPart
	results chan downloadResult
	workers sync.WaitGroup
	bodies  activeBodySet

	readMu        sync.Mutex
	totalParts    int
	nextScheduled int
	nextPart      int
	jobsClosed    bool
	pending       map[int][]byte
	current       []byte
	currentOffset int
	hadCurrent    bool
	terminal      error

	stopBodyCloser func() bool
	closeOnce      sync.Once
}

func newTransferReader(parent context.Context, manager *transferManager, bucket, key string, metadata Metadata) (*transferReader, error) {
	partCount := ceilDiv(metadata.Size, manager.options.partSize)
	if partCount <= 0 || int64(int(partCount)) != partCount {
		return nil, fmt.Errorf("S3 object requires unsupported download part count %d", partCount)
	}
	ctx, cancel := context.WithCancelCause(parent)
	r := &transferReader{
		ctx:        ctx,
		cancel:     cancel,
		manager:    manager,
		bucket:     bucket,
		key:        key,
		size:       metadata.Size,
		etag:       metadata.ETag,
		version:    metadata.VersionID,
		jobs:       make(chan downloadPart, manager.options.concurrency),
		results:    make(chan downloadResult, manager.options.concurrency),
		totalParts: int(partCount),
		pending:    make(map[int][]byte),
		bodies:     newActiveBodySet(),
	}
	r.stopBodyCloser = context.AfterFunc(ctx, r.bodies.closeAll)
	for range manager.options.concurrency {
		r.workers.Go(r.downloadWorker)
	}
	initial := min(manager.options.concurrency, r.totalParts)
	for range initial {
		if err := r.scheduleNext(); err != nil {
			r.cancel(err)
			r.bodies.closeAll()
			r.workers.Wait()
			_ = r.stopBodyCloser()
			return nil, err
		}
	}
	return r, nil
}

func (r *transferReader) scheduleNext() error {
	if r.nextScheduled >= r.totalParts {
		if !r.jobsClosed {
			close(r.jobs)
			r.jobsClosed = true
		}
		return nil
	}
	start := int64(r.nextScheduled) * r.manager.options.partSize
	end := min(r.size-1, start+r.manager.options.partSize-1)
	part := downloadPart{index: r.nextScheduled, start: start, end: end}
	select {
	case r.jobs <- part:
		r.nextScheduled++
		if r.nextScheduled == r.totalParts {
			close(r.jobs)
			r.jobsClosed = true
		}
		return nil
	case <-r.ctx.Done():
		return context.Cause(r.ctx)
	}
}

func (r *transferReader) downloadWorker() {
	for {
		select {
		case <-r.ctx.Done():
			return
		case part, ok := <-r.jobs:
			if !ok {
				return
			}
			body, err := r.fetchPart(part)
			if err != nil {
				r.cancel(err)
				return
			}
			select {
			case r.results <- downloadResult{index: part.index, body: body}:
			case <-r.ctx.Done():
				return
			}
		}
	}
}

func (r *transferReader) fetchPart(part downloadPart) ([]byte, error) {
	rangeValue := fmt.Sprintf("bytes=%d-%d", part.start, part.end)
	expectedLength := part.end - part.start + 1
	var lastErr error
	for attempt := 1; attempt <= r.manager.options.bodyAttempts; attempt++ {
		input := &awss3.GetObjectInput{
			Bucket: new(r.bucket),
			Key:    new(r.key),
			Range:  new(rangeValue),
		}
		if strings.TrimSpace(r.version) != "" {
			input.VersionId = new(r.version)
		} else {
			input.IfMatch = new(r.etag)
		}
		out, err := r.manager.client.GetObject(r.ctx, input)
		if err != nil {
			return nil, fmt.Errorf("get S3 range %s: %w", rangeValue, err)
		}
		if out == nil || out.Body == nil {
			lastErr = fmt.Errorf("get S3 range %s returned an empty body", rangeValue)
		} else {
			body, tracked := r.bodies.add(out.Body)
			if !tracked {
				return nil, context.Cause(r.ctx)
			}
			payload, readErr := io.ReadAll(io.LimitReader(body, expectedLength+1))
			closeErr := r.bodies.remove(body)
			lastErr = errors.Join(readErr, closeErr)
			if lastErr == nil {
				lastErr = validateRangeResponse(out, payload, part.start, part.end, r.size)
			}
			if lastErr == nil {
				return payload, nil
			}
		}
		if cause := context.Cause(r.ctx); cause != nil {
			return nil, cause
		}
	}
	return nil, fmt.Errorf("get S3 range %s failed after %d attempts: %w", rangeValue, r.manager.options.bodyAttempts, lastErr)
}

func validateRangeResponse(out *awss3.GetObjectOutput, payload []byte, expectedStart, expectedEnd, expectedTotal int64) error {
	expectedLength := expectedEnd - expectedStart + 1
	if out.ContentLength == nil {
		return fmt.Errorf("range response omitted Content-Length")
	}
	if *out.ContentLength != expectedLength {
		return fmt.Errorf("range response Content-Length %d, expected %d", *out.ContentLength, expectedLength)
	}
	if out.ContentRange == nil {
		return fmt.Errorf("range response omitted Content-Range")
	}
	start, end, total, err := parseContentRange(*out.ContentRange)
	if err != nil {
		return err
	}
	if start != expectedStart || end != expectedEnd || total != expectedTotal {
		return fmt.Errorf("range response was bytes %d-%d/%d, expected bytes %d-%d/%d", start, end, total, expectedStart, expectedEnd, expectedTotal)
	}
	if int64(len(payload)) != expectedLength {
		return fmt.Errorf("range response body contained %d bytes, expected %d", len(payload), expectedLength)
	}
	return nil
}

func parseContentRange(value string) (start, end, total int64, err error) {
	value = strings.TrimSpace(value)
	if !strings.HasPrefix(value, "bytes ") {
		return 0, 0, 0, fmt.Errorf("invalid Content-Range %q", value)
	}
	rangeAndTotal := strings.Split(strings.TrimPrefix(value, "bytes "), "/")
	if len(rangeAndTotal) != 2 {
		return 0, 0, 0, fmt.Errorf("invalid Content-Range %q", value)
	}
	startAndEnd := strings.Split(rangeAndTotal[0], "-")
	if len(startAndEnd) != 2 {
		return 0, 0, 0, fmt.Errorf("invalid Content-Range %q", value)
	}
	start, err = strconv.ParseInt(startAndEnd[0], 10, 64)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("invalid Content-Range %q: %w", value, err)
	}
	end, err = strconv.ParseInt(startAndEnd[1], 10, 64)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("invalid Content-Range %q: %w", value, err)
	}
	total, err = strconv.ParseInt(rangeAndTotal[1], 10, 64)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("invalid Content-Range %q: %w", value, err)
	}
	if start < 0 || end < start || total <= end {
		return 0, 0, 0, fmt.Errorf("invalid Content-Range %q", value)
	}
	return start, end, total, nil
}

func (r *transferReader) Read(buffer []byte) (int, error) {
	r.readMu.Lock()
	defer r.readMu.Unlock()
	if r.terminal != nil {
		return 0, r.terminal
	}
	if len(buffer) == 0 {
		return 0, nil
	}

	written := 0
	for written < len(buffer) {
		if r.currentOffset == len(r.current) {
			if r.hadCurrent {
				r.current = nil
				r.currentOffset = 0
				r.hadCurrent = false
				if err := r.scheduleNext(); err != nil {
					r.terminal = err
					if written > 0 {
						return written, err
					}
					return 0, err
				}
			}
			if r.nextPart == r.totalParts {
				r.terminal = io.EOF
				r.cancel(io.EOF)
				if written > 0 {
					return written, nil
				}
				return 0, io.EOF
			}
			result, err := r.awaitPart(r.nextPart)
			if err != nil {
				r.terminal = err
				if written > 0 {
					return written, err
				}
				return 0, err
			}
			r.current = result
			r.currentOffset = 0
			r.hadCurrent = true
			r.nextPart++
		}
		n := copy(buffer[written:], r.current[r.currentOffset:])
		r.currentOffset += n
		written += n
	}
	return written, nil
}

func (r *transferReader) awaitPart(index int) ([]byte, error) {
	if body, ok := r.pending[index]; ok {
		delete(r.pending, index)
		return body, nil
	}
	for {
		select {
		case result := <-r.results:
			if result.index == index {
				return result.body, nil
			}
			r.pending[result.index] = result.body
		case <-r.ctx.Done():
			return nil, context.Cause(r.ctx)
		}
	}
}

func (r *transferReader) Close() error {
	r.closeOnce.Do(func() {
		r.cancel(errTransferReaderClosed)
		r.bodies.closeAll()
		r.workers.Wait()
		if r.stopBodyCloser != nil {
			_ = r.stopBodyCloser()
		}
		r.readMu.Lock()
		r.terminal = errTransferReaderClosed
		r.current = nil
		r.currentOffset = 0
		r.hadCurrent = false
		clear(r.pending)
		for len(r.results) > 0 {
			<-r.results
		}
		r.readMu.Unlock()
	})
	return nil
}

type managedBody struct {
	io.ReadCloser
	once sync.Once
	err  error
}

func (b *managedBody) close() error {
	b.once.Do(func() {
		b.err = b.Close()
	})
	return b.err
}

type activeBodySet struct {
	mu     sync.Mutex
	closed bool
	bodies map[*managedBody]struct{}
}

func newActiveBodySet() activeBodySet {
	return activeBodySet{bodies: make(map[*managedBody]struct{})}
}

func (s *activeBodySet) add(body io.ReadCloser) (*managedBody, bool) {
	managed := &managedBody{ReadCloser: body}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		_ = managed.close()
		return managed, false
	}
	s.bodies[managed] = struct{}{}
	s.mu.Unlock()
	return managed, true
}

func (s *activeBodySet) remove(body *managedBody) error {
	err := body.close()
	s.mu.Lock()
	delete(s.bodies, body)
	s.mu.Unlock()
	return err
}

func (s *activeBodySet) closeAll() {
	s.mu.Lock()
	s.closed = true
	bodies := make([]*managedBody, 0, len(s.bodies))
	for body := range s.bodies {
		bodies = append(bodies, body)
	}
	s.mu.Unlock()
	for _, body := range bodies {
		_ = body.close()
	}
}
