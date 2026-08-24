package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"

	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type readerAtSeeker interface {
	io.ReaderAt
	io.Seeker
}

func (m *transferManager) upload(ctx context.Context, request *uploadRequest) error {
	if request == nil {
		return fmt.Errorf("upload request is nil")
	}
	if m == nil || m.client == nil {
		return fmt.Errorf("s3 transfer manager is not configured")
	}
	if strings.TrimSpace(request.bucket) == "" {
		return fmt.Errorf("upload bucket is empty")
	}
	if strings.TrimSpace(request.key) == "" {
		return fmt.Errorf("upload key is empty")
	}
	if request.body == nil {
		return fmt.Errorf("upload body is nil")
	}

	uploadCtx, cancel := context.WithCancelCause(ctx)
	var stopCancelRead func() bool
	if request.cancelRead != nil {
		stopCancelRead = context.AfterFunc(uploadCtx, func() {
			request.cancelRead(context.Cause(uploadCtx))
		})
	}

	err := m.uploadBody(uploadCtx, cancel, request)
	if err != nil {
		cancel(err)
		if request.cancelRead != nil {
			request.cancelRead(err)
		}
	}
	if stopCancelRead != nil {
		_ = stopCancelRead()
	}
	if err == nil {
		cancel(context.Canceled)
	}
	return err
}

func (m *transferManager) uploadBody(ctx context.Context, cancel context.CancelCauseFunc, request *uploadRequest) error {
	if body, ok := request.body.(readerAtSeeker); ok {
		start, size, err := remainingSeekSize(body)
		if err != nil {
			return fmt.Errorf("determine upload body size: %w", err)
		}
		if size < m.options.multipartThreshold {
			return m.putObject(ctx, request, io.NewSectionReader(body, start, size), size)
		}
		partSize, err := m.adjustedPartSize(size)
		if err != nil {
			return err
		}
		return m.multipartUpload(ctx, cancel, request, sectionPartProducer(body, start, size, partSize))
	}

	if body, ok := request.body.(io.Seeker); ok {
		_, size, err := remainingSeekSize(body)
		if err != nil {
			return fmt.Errorf("determine upload body size: %w", err)
		}
		if size < m.options.multipartThreshold {
			return m.putObject(ctx, request, request.body, size)
		}
		partSize, err := m.adjustedPartSize(size)
		if err != nil {
			return err
		}
		return m.multipartUpload(ctx, cancel, request, m.streamPartProducer(io.LimitReader(request.body, size), partSize, size))
	}

	probeSize, err := checkedBufferSize(m.options.multipartThreshold)
	if err != nil {
		return err
	}
	probe := make([]byte, probeSize)
	n, readErr := readUntilFull(request.body, probe)
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		if cause := context.Cause(ctx); cause != nil {
			return cause
		}
		return fmt.Errorf("read upload body: %w", readErr)
	}
	if int64(n) < m.options.multipartThreshold {
		return m.putObject(ctx, request, bytes.NewReader(probe[:n]), int64(n))
	}

	body := io.MultiReader(bytes.NewReader(probe[:n]), request.body)
	return m.multipartUpload(ctx, cancel, request, m.streamPartProducer(body, m.options.partSize, -1))
}

func remainingSeekSize(seeker io.Seeker) (start int64, size int64, err error) {
	start, err = seeker.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, 0, err
	}
	end, endErr := seeker.Seek(0, io.SeekEnd)
	_, restoreErr := seeker.Seek(start, io.SeekStart)
	if endErr != nil || restoreErr != nil {
		return 0, 0, errors.Join(endErr, restoreErr)
	}
	if end < start {
		return 0, 0, fmt.Errorf("upload body end %d precedes current offset %d", end, start)
	}
	return start, end - start, nil
}

func (m *transferManager) adjustedPartSize(size int64) (int64, error) {
	if size < 0 {
		return 0, fmt.Errorf("upload size must be non-negative")
	}
	if size > m.options.maxObjectSize {
		return 0, fmt.Errorf("upload size %d exceeds S3 object limit %d", size, m.options.maxObjectSize)
	}
	partSize := m.options.partSize
	parts := ceilDiv(size, partSize)
	if parts > int64(m.options.maxUploadParts) {
		partSize = ceilDiv(size, int64(m.options.maxUploadParts))
	}
	if partSize > m.options.maxPartSize {
		return 0, fmt.Errorf("required upload part size %d exceeds S3 part limit %d", partSize, m.options.maxPartSize)
	}
	return partSize, nil
}

func ceilDiv(value, divisor int64) int64 {
	if value == 0 {
		return 0
	}
	return 1 + (value-1)/divisor
}

func (m *transferManager) putObject(ctx context.Context, request *uploadRequest, body io.Reader, size int64) error {
	input := &awss3.PutObjectInput{
		Bucket:               new(request.bucket),
		Key:                  new(request.key),
		Body:                 body,
		CacheControl:         request.cacheControl,
		ChecksumAlgorithm:    s3types.ChecksumAlgorithmCrc32,
		ContentLength:        new(size),
		ContentType:          request.contentType,
		Metadata:             request.metadata,
		ServerSideEncryption: request.serverSideEncryption,
		SSEKMSKeyId:          request.sseKMSKeyID,
		Tagging:              request.tagging,
	}
	if _, err := m.client.PutObject(ctx, input); err != nil {
		return fmt.Errorf("put S3 object: %w", err)
	}
	return nil
}

type uploadPartTask struct {
	number  int32
	body    io.Reader
	size    int64
	release func()
}

type uploadPartProducer func(context.Context, chan<- uploadPartTask) (total int64, count int32, err error)

func sectionPartProducer(body io.ReaderAt, start, size, partSize int64) uploadPartProducer {
	return func(ctx context.Context, jobs chan<- uploadPartTask) (int64, int32, error) {
		var count int32
		for offset := int64(0); offset < size; offset += partSize {
			length := min(partSize, size-offset)
			count++
			task := uploadPartTask{
				number:  count,
				body:    io.NewSectionReader(body, start+offset, length),
				size:    length,
				release: func() {},
			}
			select {
			case jobs <- task:
			case <-ctx.Done():
				return offset, count - 1, context.Cause(ctx)
			}
		}
		return size, count, nil
	}
}

func (m *transferManager) streamPartProducer(body io.Reader, partSize, expectedSize int64) uploadPartProducer {
	return func(ctx context.Context, jobs chan<- uploadPartTask) (int64, int32, error) {
		bufferSize, err := checkedBufferSize(partSize)
		if err != nil {
			return 0, 0, err
		}
		pool := newLazyBufferPool(bufferSize, m.options.concurrency+1)
		var total int64
		var count int32
		for {
			buffer, err := pool.get(ctx)
			if err != nil {
				return total, count, err
			}
			n, readErr := readUntilFull(body, buffer)
			if cause := context.Cause(ctx); cause != nil {
				pool.put(buffer)
				return total, count, cause
			}
			if readErr != nil && !errors.Is(readErr, io.EOF) {
				pool.put(buffer)
				return total, count, fmt.Errorf("read multipart upload body: %w", readErr)
			}
			if n == 0 {
				pool.put(buffer)
				break
			}
			if count == m.options.maxUploadParts {
				pool.put(buffer)
				return total, count, fmt.Errorf("upload exceeds maximum of %d parts", m.options.maxUploadParts)
			}
			if int64(n) > m.options.maxObjectSize-total {
				pool.put(buffer)
				return total, count, fmt.Errorf("upload exceeds S3 object limit %d", m.options.maxObjectSize)
			}

			count++
			total += int64(n)
			taskBuffer := buffer
			task := uploadPartTask{
				number: count,
				body:   bytes.NewReader(taskBuffer[:n]),
				size:   int64(n),
				release: func() {
					pool.put(taskBuffer)
				},
			}
			select {
			case jobs <- task:
			case <-ctx.Done():
				task.release()
				return total - int64(n), count - 1, context.Cause(ctx)
			}
			if errors.Is(readErr, io.EOF) {
				break
			}
		}
		if expectedSize >= 0 && total != expectedSize {
			return total, count, fmt.Errorf("upload body size changed: read %d bytes, expected %d", total, expectedSize)
		}
		return total, count, nil
	}
}

func (m *transferManager) multipartUpload(ctx context.Context, cancel context.CancelCauseFunc, request *uploadRequest, produce uploadPartProducer) error {
	createOut, err := m.client.CreateMultipartUpload(ctx, &awss3.CreateMultipartUploadInput{
		Bucket:               new(request.bucket),
		Key:                  new(request.key),
		CacheControl:         request.cacheControl,
		ChecksumAlgorithm:    s3types.ChecksumAlgorithmCrc32,
		ContentType:          request.contentType,
		Metadata:             request.metadata,
		ServerSideEncryption: request.serverSideEncryption,
		SSEKMSKeyId:          request.sseKMSKeyID,
		Tagging:              request.tagging,
	})
	if err != nil {
		return fmt.Errorf("create multipart upload: %w", err)
	}
	if createOut == nil || createOut.UploadId == nil || strings.TrimSpace(*createOut.UploadId) == "" {
		return fmt.Errorf("create multipart upload returned an empty upload ID")
	}
	uploadID := *createOut.UploadId

	jobs := make(chan uploadPartTask, m.options.concurrency)
	var workers sync.WaitGroup
	var partsMu sync.Mutex
	parts := make([]s3types.CompletedPart, 0)
	for range m.options.concurrency {
		workers.Go(func() {
			for task := range jobs {
				func() {
					defer task.release()
					if context.Cause(ctx) != nil {
						return
					}
					out, partErr := m.client.UploadPart(ctx, &awss3.UploadPartInput{
						Bucket:            new(request.bucket),
						Key:               new(request.key),
						UploadId:          new(uploadID),
						PartNumber:        new(task.number),
						Body:              task.body,
						ChecksumAlgorithm: s3types.ChecksumAlgorithmCrc32,
						ContentLength:     new(task.size),
					})
					if partErr != nil {
						cancel(fmt.Errorf("upload part %d: %w", task.number, partErr))
						return
					}
					if out == nil || out.ETag == nil || strings.TrimSpace(*out.ETag) == "" {
						cancel(fmt.Errorf("upload part %d returned an empty ETag", task.number))
						return
					}
					partsMu.Lock()
					parts = append(parts, s3types.CompletedPart{
						ChecksumCRC32: out.ChecksumCRC32,
						ETag:          out.ETag,
						PartNumber:    new(task.number),
					})
					partsMu.Unlock()
				}()
			}
		})
	}

	total, count, produceErr := produce(ctx, jobs)
	if produceErr != nil {
		cancel(produceErr)
	}
	close(jobs)
	workers.Wait()

	rootErr := produceErr
	if rootErr == nil {
		rootErr = context.Cause(ctx)
	}
	if rootErr != nil {
		return m.abortMultipart(ctx, request, uploadID, rootErr)
	}

	partsMu.Lock()
	completed := append([]s3types.CompletedPart(nil), parts...)
	partsMu.Unlock()
	if int32(len(completed)) != count {
		rootErr = fmt.Errorf("multipart upload completed %d of %d parts", len(completed), count)
		return m.abortMultipart(ctx, request, uploadID, rootErr)
	}
	sort.Slice(completed, func(i, j int) bool {
		return *completed[i].PartNumber < *completed[j].PartNumber
	})

	_, err = m.client.CompleteMultipartUpload(ctx, &awss3.CompleteMultipartUploadInput{
		Bucket:        new(request.bucket),
		Key:           new(request.key),
		UploadId:      new(uploadID),
		MpuObjectSize: new(total),
		MultipartUpload: &s3types.CompletedMultipartUpload{
			Parts: completed,
		},
	})
	if err != nil {
		rootErr = fmt.Errorf("complete multipart upload: %w", err)
		cancel(rootErr)
		return m.abortMultipart(ctx, request, uploadID, rootErr)
	}
	return nil
}

func (m *transferManager) abortMultipart(ctx context.Context, request *uploadRequest, uploadID string, rootErr error) error {
	cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), m.options.abortTimeout)
	defer cleanupCancel()
	_, cleanupErr := m.client.AbortMultipartUpload(cleanupCtx, &awss3.AbortMultipartUploadInput{
		Bucket:   new(request.bucket),
		Key:      new(request.key),
		UploadId: new(uploadID),
	})
	if cleanupErr != nil {
		return newMultipartCleanupError(rootErr, cleanupErr)
	}
	return rootErr
}

func checkedBufferSize(size int64) (int, error) {
	if size <= 0 || int64(int(size)) != size {
		return 0, fmt.Errorf("transfer buffer size %d is not supported on this platform", size)
	}
	return int(size), nil
}

func readUntilFull(reader io.Reader, buffer []byte) (int, error) {
	offset := 0
	emptyReads := 0
	for offset < len(buffer) {
		n, err := reader.Read(buffer[offset:])
		if n < 0 || n > len(buffer)-offset {
			return offset, fmt.Errorf("invalid reader count %d", n)
		}
		if n > 0 {
			offset += n
			emptyReads = 0
		} else if err == nil {
			emptyReads++
			if emptyReads >= 100 {
				return offset, io.ErrNoProgress
			}
		}
		if err != nil {
			return offset, err
		}
	}
	return offset, nil
}

type lazyBufferPool struct {
	size      int
	limit     int
	allocated int
	free      chan []byte
}

func newLazyBufferPool(size, limit int) *lazyBufferPool {
	return &lazyBufferPool{size: size, limit: limit, free: make(chan []byte, limit)}
}

func (p *lazyBufferPool) get(ctx context.Context) ([]byte, error) {
	select {
	case buffer := <-p.free:
		return buffer, nil
	default:
	}
	if p.allocated < p.limit {
		p.allocated++
		return make([]byte, p.size), nil
	}
	select {
	case buffer := <-p.free:
		return buffer, nil
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	}
}

func (p *lazyBufferPool) put(buffer []byte) {
	p.free <- buffer[:p.size]
}
