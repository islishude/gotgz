package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/islishude/gotgz/packages/locator"
)

func TestTransferUploadSmallNonSeekableUsesPutObject(t *testing.T) {
	payload := "small"
	var gotBody string
	client := &fakeTransferS3Client{
		putObjectFn: func(_ context.Context, input *awss3.PutObjectInput, _ ...func(*awss3.Options)) (*awss3.PutObjectOutput, error) {
			body, err := io.ReadAll(input.Body)
			if err != nil {
				return nil, err
			}
			gotBody = string(body)
			if aws.ToInt64(input.ContentLength) != int64(len(payload)) {
				return nil, fmt.Errorf("ContentLength = %d", aws.ToInt64(input.ContentLength))
			}
			if input.ChecksumAlgorithm != s3types.ChecksumAlgorithmCrc32 {
				return nil, fmt.Errorf("ChecksumAlgorithm = %q", input.ChecksumAlgorithm)
			}
			if input.Metadata["owner"] != "archive" || aws.ToString(input.ContentType) != "application/test" || aws.ToString(input.CacheControl) != "no-store" || aws.ToString(input.Tagging) != "team=storage" {
				return nil, fmt.Errorf("upload headers were not preserved")
			}
			if input.ServerSideEncryption != s3types.ServerSideEncryptionAes256 {
				return nil, fmt.Errorf("SSE = %q", input.ServerSideEncryption)
			}
			return &awss3.PutObjectOutput{}, nil
		},
	}
	manager := newTransferManager(client, transferOptions{partSize: 5, multipartThreshold: 8, concurrency: 2})
	err := manager.upload(context.Background(), &uploadRequest{
		bucket:               "bucket",
		key:                  "key",
		body:                 readerOnly{Reader: strings.NewReader(payload)},
		metadata:             map[string]string{"owner": "archive"},
		contentType:          new("application/test"),
		cacheControl:         new("no-store"),
		tagging:              new("team=storage"),
		serverSideEncryption: s3types.ServerSideEncryptionAes256,
	})
	if err != nil {
		t.Fatalf("upload() error = %v", err)
	}
	if gotBody != payload {
		t.Fatalf("PutObject body = %q, want %q", gotBody, payload)
	}
}

func TestTransferUploadThresholdUsesConcurrentMultipart(t *testing.T) {
	payload := "abcdefghijklm"
	firstTwoStarted := make(chan struct{})
	var mu sync.Mutex
	parts := make(map[int32]string)
	active := 0
	maxActive := 0
	firstTwoCount := 0
	client := &fakeTransferS3Client{
		createMultipartUploadFn: func(_ context.Context, input *awss3.CreateMultipartUploadInput, _ ...func(*awss3.Options)) (*awss3.CreateMultipartUploadOutput, error) {
			if input.ChecksumAlgorithm != s3types.ChecksumAlgorithmCrc32 {
				return nil, fmt.Errorf("create checksum = %q", input.ChecksumAlgorithm)
			}
			return &awss3.CreateMultipartUploadOutput{UploadId: new("upload-1")}, nil
		},
		uploadPartFn: func(_ context.Context, input *awss3.UploadPartInput, _ ...func(*awss3.Options)) (*awss3.UploadPartOutput, error) {
			partNumber := aws.ToInt32(input.PartNumber)
			mu.Lock()
			active++
			maxActive = max(maxActive, active)
			if partNumber <= 2 {
				firstTwoCount++
				if firstTwoCount == 2 {
					close(firstTwoStarted)
				}
			}
			mu.Unlock()
			if partNumber <= 2 {
				select {
				case <-firstTwoStarted:
				case <-time.After(2 * time.Second):
					return nil, fmt.Errorf("first two parts did not start concurrently")
				}
			}
			body, err := io.ReadAll(input.Body)
			mu.Lock()
			active--
			parts[partNumber] = string(body)
			mu.Unlock()
			if err != nil {
				return nil, err
			}
			return &awss3.UploadPartOutput{ETag: new(fmt.Sprintf("etag-%d", partNumber)), ChecksumCRC32: new(fmt.Sprintf("crc-%d", partNumber))}, nil
		},
		completeMultipartUploadFn: func(_ context.Context, input *awss3.CompleteMultipartUploadInput, _ ...func(*awss3.Options)) (*awss3.CompleteMultipartUploadOutput, error) {
			if aws.ToInt64(input.MpuObjectSize) != int64(len(payload)) {
				return nil, fmt.Errorf("MpuObjectSize = %d", aws.ToInt64(input.MpuObjectSize))
			}
			for index, part := range input.MultipartUpload.Parts {
				want := int32(index + 1)
				if aws.ToInt32(part.PartNumber) != want || aws.ToString(part.ChecksumCRC32) != fmt.Sprintf("crc-%d", want) {
					return nil, fmt.Errorf("completed part %d = %+v", index, part)
				}
			}
			return &awss3.CompleteMultipartUploadOutput{}, nil
		},
	}
	manager := newTransferManager(client, transferOptions{partSize: 5, multipartThreshold: 8, concurrency: 2})
	if err := manager.upload(context.Background(), &uploadRequest{bucket: "bucket", key: "key", body: readerOnly{Reader: strings.NewReader(payload)}}); err != nil {
		t.Fatalf("upload() error = %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if maxActive != 2 {
		t.Fatalf("max concurrent UploadPart calls = %d, want 2", maxActive)
	}
	if got := parts[1] + parts[2] + parts[3]; got != payload {
		t.Fatalf("multipart payload = %q, want %q", got, payload)
	}
}

func TestTransferUploadSeekableUsesReaderAtPartsFromCurrentOffset(t *testing.T) {
	body := newTrackingReaderAtSeeker([]byte("skip-payload"))
	if _, err := body.Seek(5, io.SeekStart); err != nil {
		t.Fatalf("Seek() error = %v", err)
	}
	var mu sync.Mutex
	parts := make(map[int32]string)
	client := successfulMultipartClient(func(input *awss3.UploadPartInput) error {
		payload, err := io.ReadAll(input.Body)
		if err != nil {
			return err
		}
		mu.Lock()
		parts[aws.ToInt32(input.PartNumber)] = string(payload)
		mu.Unlock()
		return nil
	})
	manager := newTransferManager(client, transferOptions{partSize: 3, multipartThreshold: 4, concurrency: 2})
	if err := manager.upload(context.Background(), &uploadRequest{bucket: "bucket", key: "key", body: body}); err != nil {
		t.Fatalf("upload() error = %v", err)
	}
	if body.readCalls != 0 {
		t.Fatalf("sequential Read() calls = %d, want 0", body.readCalls)
	}
	mu.Lock()
	defer mu.Unlock()
	if got := parts[1] + parts[2] + parts[3]; got != "payload" {
		t.Fatalf("multipart payload = %q, want payload", got)
	}
}

func TestTransferUploadPartFailureAbortsAndPreservesRootCause(t *testing.T) {
	wantErr := errors.New("part failed")
	abortCalls := 0
	client := &fakeTransferS3Client{
		createMultipartUploadFn: func(context.Context, *awss3.CreateMultipartUploadInput, ...func(*awss3.Options)) (*awss3.CreateMultipartUploadOutput, error) {
			return &awss3.CreateMultipartUploadOutput{UploadId: new("upload-1")}, nil
		},
		uploadPartFn: func(context.Context, *awss3.UploadPartInput, ...func(*awss3.Options)) (*awss3.UploadPartOutput, error) {
			return nil, wantErr
		},
		abortMultipartUploadFn: func(ctx context.Context, _ *awss3.AbortMultipartUploadInput, _ ...func(*awss3.Options)) (*awss3.AbortMultipartUploadOutput, error) {
			abortCalls++
			if ctx.Err() != nil {
				return nil, fmt.Errorf("abort context was canceled: %w", ctx.Err())
			}
			return &awss3.AbortMultipartUploadOutput{}, nil
		},
	}
	manager := newTransferManager(client, transferOptions{partSize: 3, multipartThreshold: 4, concurrency: 1})
	err := manager.upload(context.Background(), &uploadRequest{bucket: "bucket", key: "key", body: readerOnly{Reader: strings.NewReader("abcdefgh")}})
	if !errors.Is(err, wantErr) {
		t.Fatalf("upload() error = %v, want %v", err, wantErr)
	}
	if abortCalls != 1 {
		t.Fatalf("AbortMultipartUpload() calls = %d, want 1", abortCalls)
	}
}

func TestTransferUploadCancellationUsesDetachedAbortContext(t *testing.T) {
	type contextKey struct{}
	ctx, cancelParent := context.WithCancel(context.WithValue(context.Background(), contextKey{}, "kept"))
	partStarted := make(chan struct{})
	client := &fakeTransferS3Client{
		createMultipartUploadFn: func(context.Context, *awss3.CreateMultipartUploadInput, ...func(*awss3.Options)) (*awss3.CreateMultipartUploadOutput, error) {
			return &awss3.CreateMultipartUploadOutput{UploadId: new("upload-1")}, nil
		},
		uploadPartFn: func(ctx context.Context, _ *awss3.UploadPartInput, _ ...func(*awss3.Options)) (*awss3.UploadPartOutput, error) {
			close(partStarted)
			<-ctx.Done()
			return nil, ctx.Err()
		},
		abortMultipartUploadFn: func(ctx context.Context, _ *awss3.AbortMultipartUploadInput, _ ...func(*awss3.Options)) (*awss3.AbortMultipartUploadOutput, error) {
			if ctx.Err() != nil {
				return nil, fmt.Errorf("abort context was canceled: %w", ctx.Err())
			}
			if got := ctx.Value(contextKey{}); got != "kept" {
				return nil, fmt.Errorf("abort context value = %v", got)
			}
			return &awss3.AbortMultipartUploadOutput{}, nil
		},
	}
	manager := newTransferManager(client, transferOptions{partSize: 3, multipartThreshold: 4, concurrency: 1})
	errCh := make(chan error, 1)
	go func() {
		errCh <- manager.upload(ctx, &uploadRequest{bucket: "bucket", key: "key", body: readerOnly{Reader: strings.NewReader("abcdefgh")}})
	}()
	select {
	case <-partStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("UploadPart did not start")
	}
	cancelParent()
	if err := <-errCh; !errors.Is(err, context.Canceled) {
		t.Fatalf("upload() error = %v, want context.Canceled", err)
	}
}

func TestTransferUploadJoinsAbortFailure(t *testing.T) {
	wantErr := errors.New("part failed")
	wantAbortErr := errors.New("abort failed")
	client := &fakeTransferS3Client{
		createMultipartUploadFn: func(context.Context, *awss3.CreateMultipartUploadInput, ...func(*awss3.Options)) (*awss3.CreateMultipartUploadOutput, error) {
			return &awss3.CreateMultipartUploadOutput{UploadId: new("upload-1")}, nil
		},
		uploadPartFn: func(context.Context, *awss3.UploadPartInput, ...func(*awss3.Options)) (*awss3.UploadPartOutput, error) {
			return nil, wantErr
		},
		abortMultipartUploadFn: func(context.Context, *awss3.AbortMultipartUploadInput, ...func(*awss3.Options)) (*awss3.AbortMultipartUploadOutput, error) {
			return nil, wantAbortErr
		},
	}
	manager := newTransferManager(client, transferOptions{partSize: 3, multipartThreshold: 4, concurrency: 1})
	err := manager.upload(context.Background(), &uploadRequest{bucket: "bucket", key: "key", body: readerOnly{Reader: strings.NewReader("abcdefgh")}})
	if !errors.Is(err, wantErr) || !errors.Is(err, wantAbortErr) {
		t.Fatalf("upload() error = %v, want both failures", err)
	}
	if got := suppressExpectedAbortError(err, wantErr); !errors.Is(got, wantAbortErr) || errors.Is(got, wantErr) {
		t.Fatalf("suppressExpectedAbortError() = %v, want cleanup error only", got)
	}
}

func TestSuppressExpectedAbortErrorPreservesCleanupAfterContextCancellation(t *testing.T) {
	cleanupErr := errors.New("abort failed")
	uploadErr := newMultipartCleanupError(context.Canceled, cleanupErr)
	got := suppressExpectedAbortError(uploadErr, errors.New("caller stopped"))
	if !errors.Is(got, cleanupErr) {
		t.Fatalf("suppressExpectedAbortError() = %v, want cleanup error", got)
	}
	if errors.Is(got, context.Canceled) {
		t.Fatalf("suppressExpectedAbortError() retained expected cancellation: %v", got)
	}
}

func TestTransferUploadReadFailureAbortsWithoutCompleting(t *testing.T) {
	wantErr := errors.New("source failed")
	completed := false
	aborted := false
	client := &fakeTransferS3Client{
		createMultipartUploadFn: func(context.Context, *awss3.CreateMultipartUploadInput, ...func(*awss3.Options)) (*awss3.CreateMultipartUploadOutput, error) {
			return &awss3.CreateMultipartUploadOutput{UploadId: new("upload-1")}, nil
		},
		uploadPartFn: func(_ context.Context, input *awss3.UploadPartInput, _ ...func(*awss3.Options)) (*awss3.UploadPartOutput, error) {
			return &awss3.UploadPartOutput{ETag: new(fmt.Sprintf("etag-%d", aws.ToInt32(input.PartNumber))), ChecksumCRC32: new("crc")}, nil
		},
		completeMultipartUploadFn: func(context.Context, *awss3.CompleteMultipartUploadInput, ...func(*awss3.Options)) (*awss3.CompleteMultipartUploadOutput, error) {
			completed = true
			return &awss3.CompleteMultipartUploadOutput{}, nil
		},
		abortMultipartUploadFn: func(context.Context, *awss3.AbortMultipartUploadInput, ...func(*awss3.Options)) (*awss3.AbortMultipartUploadOutput, error) {
			aborted = true
			return &awss3.AbortMultipartUploadOutput{}, nil
		},
	}
	manager := newTransferManager(client, transferOptions{partSize: 3, multipartThreshold: 4, concurrency: 1})
	err := manager.upload(context.Background(), &uploadRequest{bucket: "bucket", key: "key", body: &dataThenErrorReader{data: []byte("abcd"), err: wantErr}})
	if !errors.Is(err, wantErr) {
		t.Fatalf("upload() error = %v, want %v", err, wantErr)
	}
	if completed || !aborted {
		t.Fatalf("completed = %v aborted = %v", completed, aborted)
	}
}

func TestTransferUploadCompleteFailureAborts(t *testing.T) {
	wantErr := errors.New("complete failed")
	aborted := false
	client := successfulMultipartClient(func(*awss3.UploadPartInput) error { return nil })
	client.completeMultipartUploadFn = func(context.Context, *awss3.CompleteMultipartUploadInput, ...func(*awss3.Options)) (*awss3.CompleteMultipartUploadOutput, error) {
		return nil, wantErr
	}
	client.abortMultipartUploadFn = func(context.Context, *awss3.AbortMultipartUploadInput, ...func(*awss3.Options)) (*awss3.AbortMultipartUploadOutput, error) {
		aborted = true
		return &awss3.AbortMultipartUploadOutput{}, nil
	}
	manager := newTransferManager(client, transferOptions{partSize: 3, multipartThreshold: 4, concurrency: 2})
	err := manager.upload(context.Background(), &uploadRequest{bucket: "bucket", key: "key", body: readerOnly{Reader: strings.NewReader("abcdefgh")}})
	if !errors.Is(err, wantErr) {
		t.Fatalf("upload() error = %v, want %v", err, wantErr)
	}
	if !aborted {
		t.Fatal("CompleteMultipartUpload failure did not abort")
	}
}

func TestTransferUploadUnknownSizeEnforcesPartLimit(t *testing.T) {
	aborted := false
	client := successfulMultipartClient(func(*awss3.UploadPartInput) error { return nil })
	client.abortMultipartUploadFn = func(context.Context, *awss3.AbortMultipartUploadInput, ...func(*awss3.Options)) (*awss3.AbortMultipartUploadOutput, error) {
		aborted = true
		return &awss3.AbortMultipartUploadOutput{}, nil
	}
	manager := newTransferManager(client, transferOptions{
		partSize:           3,
		multipartThreshold: 1,
		concurrency:        1,
		maxUploadParts:     2,
	})
	err := manager.upload(context.Background(), &uploadRequest{bucket: "bucket", key: "key", body: readerOnly{Reader: strings.NewReader("abcdefg")}})
	if err == nil || !strings.Contains(err.Error(), "maximum of 2 parts") {
		t.Fatalf("upload() error = %v, want part limit", err)
	}
	if !aborted {
		t.Fatal("part limit failure did not abort")
	}
}

func TestTransferUploadKnownSizeAdjustsPartSize(t *testing.T) {
	body := newTrackingReaderAtSeeker([]byte("abcdefg"))
	partSizes := make(map[int32]int64)
	var mu sync.Mutex
	client := successfulMultipartClient(func(input *awss3.UploadPartInput) error {
		mu.Lock()
		partSizes[aws.ToInt32(input.PartNumber)] = aws.ToInt64(input.ContentLength)
		mu.Unlock()
		return nil
	})
	manager := newTransferManager(client, transferOptions{
		partSize:           3,
		multipartThreshold: 1,
		concurrency:        2,
		maxUploadParts:     2,
		maxPartSize:        10,
	})
	if err := manager.upload(context.Background(), &uploadRequest{bucket: "bucket", key: "key", body: body}); err != nil {
		t.Fatalf("upload() error = %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(partSizes) != 2 || partSizes[1] != 4 || partSizes[2] != 3 {
		t.Fatalf("part sizes = %#v, want {1:4, 2:3}", partSizes)
	}
}

func TestBeginWriterUploadFailureUnblocksWriteWithRootCause(t *testing.T) {
	wantErr := errors.New("part failed")
	client := &fakeTransferS3Client{
		createMultipartUploadFn: func(context.Context, *awss3.CreateMultipartUploadInput, ...func(*awss3.Options)) (*awss3.CreateMultipartUploadOutput, error) {
			return &awss3.CreateMultipartUploadOutput{UploadId: new("upload-1")}, nil
		},
		uploadPartFn: func(context.Context, *awss3.UploadPartInput, ...func(*awss3.Options)) (*awss3.UploadPartOutput, error) {
			return nil, wantErr
		},
		abortMultipartUploadFn: func(context.Context, *awss3.AbortMultipartUploadInput, ...func(*awss3.Options)) (*awss3.AbortMultipartUploadOutput, error) {
			return &awss3.AbortMultipartUploadOutput{}, nil
		},
	}
	store := &Store{
		client:    client,
		transfers: newTransferManager(client, transferOptions{partSize: 1, multipartThreshold: 1, concurrency: 1}),
		settings:  Settings{SSE: "none"},
	}
	writer, err := store.BeginWriter(context.Background(), locator.Ref{Kind: locator.KindS3, Bucket: "bucket", Key: "key"}, nil)
	if err != nil {
		t.Fatalf("BeginWriter() error = %v", err)
	}
	if _, err := writer.Write([]byte("long payload")); !errors.Is(err, wantErr) {
		t.Fatalf("Write() error = %v, want %v", err, wantErr)
	}
	if err := writer.Commit(); !errors.Is(err, wantErr) {
		t.Fatalf("Commit() error = %v, want %v", err, wantErr)
	}
}

func successfulMultipartClient(validatePart func(*awss3.UploadPartInput) error) *fakeTransferS3Client {
	return &fakeTransferS3Client{
		createMultipartUploadFn: func(context.Context, *awss3.CreateMultipartUploadInput, ...func(*awss3.Options)) (*awss3.CreateMultipartUploadOutput, error) {
			return &awss3.CreateMultipartUploadOutput{UploadId: new("upload-1")}, nil
		},
		uploadPartFn: func(_ context.Context, input *awss3.UploadPartInput, _ ...func(*awss3.Options)) (*awss3.UploadPartOutput, error) {
			if err := validatePart(input); err != nil {
				return nil, err
			}
			partNumber := aws.ToInt32(input.PartNumber)
			return &awss3.UploadPartOutput{ETag: new(fmt.Sprintf("etag-%d", partNumber)), ChecksumCRC32: new("crc")}, nil
		},
		completeMultipartUploadFn: func(context.Context, *awss3.CompleteMultipartUploadInput, ...func(*awss3.Options)) (*awss3.CompleteMultipartUploadOutput, error) {
			return &awss3.CompleteMultipartUploadOutput{}, nil
		},
	}
}

type readerOnly struct{ io.Reader }

type trackingReaderAtSeeker struct {
	reader    *bytes.Reader
	readCalls int
}

func newTrackingReaderAtSeeker(payload []byte) *trackingReaderAtSeeker {
	return &trackingReaderAtSeeker{reader: bytes.NewReader(payload)}
}

func (r *trackingReaderAtSeeker) Read(buffer []byte) (int, error) {
	r.readCalls++
	return r.reader.Read(buffer)
}

func (r *trackingReaderAtSeeker) ReadAt(buffer []byte, offset int64) (int, error) {
	return r.reader.ReadAt(buffer, offset)
}

func (r *trackingReaderAtSeeker) Seek(offset int64, whence int) (int64, error) {
	return r.reader.Seek(offset, whence)
}

type dataThenErrorReader struct {
	data []byte
	err  error
}

func (r *dataThenErrorReader) Read(buffer []byte) (int, error) {
	if len(r.data) > 0 {
		n := copy(buffer, r.data)
		r.data = r.data[n:]
		return n, nil
	}
	return 0, r.err
}
