package s3

import (
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
	"github.com/islishude/gotgz/packages/locator"
)

func TestTransferReaderOrdersConcurrentVersionedRanges(t *testing.T) {
	payload := "abcdefghijkl"
	firstGate := make(chan struct{})
	secondReady := make(chan struct{})
	var readyOnce sync.Once
	client := &fakeTransferS3Client{
		headObjectFn: func(context.Context, *awss3.HeadObjectInput, ...func(*awss3.Options)) (*awss3.HeadObjectOutput, error) {
			return &awss3.HeadObjectOutput{
				ContentLength: new(int64(len(payload))),
				ETag:          new(`"etag"`),
				VersionId:     new("version-1"),
			}, nil
		},
		getObjectFn: func(_ context.Context, input *awss3.GetObjectInput, _ ...func(*awss3.Options)) (*awss3.GetObjectOutput, error) {
			if got := aws.ToString(input.VersionId); got != "version-1" {
				return nil, fmt.Errorf("VersionId = %q", got)
			}
			if input.IfMatch != nil {
				return nil, fmt.Errorf("unexpected IfMatch %q", aws.ToString(input.IfMatch))
			}
			start, end, err := parseRequestRange(aws.ToString(input.Range))
			if err != nil {
				return nil, err
			}
			switch start {
			case 0:
				<-firstGate
			case 4:
				readyOnce.Do(func() { close(secondReady) })
			}
			return testRangeOutput(payload, start, end), nil
		},
	}
	store := &Store{client: client, transfers: newTestTransferManager(client, 4, 2)}
	reader, _, err := store.OpenReader(context.Background(), locator.Ref{Kind: locator.KindS3, Bucket: "bucket", Key: "key"})
	if err != nil {
		t.Fatalf("OpenReader() error = %v", err)
	}
	defer reader.Close() //nolint:errcheck

	select {
	case <-secondReady:
	case <-time.After(2 * time.Second):
		t.Fatal("second range was not fetched concurrently")
	}
	close(firstGate)
	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if string(got) != payload {
		t.Fatalf("payload = %q, want %q", got, payload)
	}
}

func TestTransferReaderRetriesInvalidRangeBeforePublishing(t *testing.T) {
	payload := "abcdef"
	var mu sync.Mutex
	calls := 0
	client := &fakeTransferS3Client{
		headObjectFn: func(context.Context, *awss3.HeadObjectInput, ...func(*awss3.Options)) (*awss3.HeadObjectOutput, error) {
			return &awss3.HeadObjectOutput{ContentLength: new(int64(len(payload))), ETag: new(`"etag"`)}, nil
		},
		getObjectFn: func(_ context.Context, input *awss3.GetObjectInput, _ ...func(*awss3.Options)) (*awss3.GetObjectOutput, error) {
			start, end, err := parseRequestRange(aws.ToString(input.Range))
			if err != nil {
				return nil, err
			}
			mu.Lock()
			calls++
			call := calls
			mu.Unlock()
			out := testRangeOutput(payload, start, end)
			if call == 1 {
				out.ContentRange = new(fmt.Sprintf("bytes %d-%d/%d", start+1, end, len(payload)))
			}
			return out, nil
		},
	}
	manager := newTransferManager(client, transferOptions{partSize: 6, concurrency: 1, bodyAttempts: 2})
	reader, _, err := manager.openReader(context.Background(), "bucket", "key")
	if err != nil {
		t.Fatalf("openReader() error = %v", err)
	}
	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if string(got) != payload {
		t.Fatalf("payload = %q, want %q", got, payload)
	}
	mu.Lock()
	defer mu.Unlock()
	if calls != 2 {
		t.Fatalf("GetObject() calls = %d, want 2", calls)
	}
}

func TestTransferReaderRetriesBodyReadFailure(t *testing.T) {
	wantErr := errors.New("body failed")
	payload := "abcdef"
	var calls int
	client := &fakeTransferS3Client{
		headObjectFn: func(context.Context, *awss3.HeadObjectInput, ...func(*awss3.Options)) (*awss3.HeadObjectOutput, error) {
			return &awss3.HeadObjectOutput{ContentLength: new(int64(len(payload))), ETag: new(`"etag"`)}, nil
		},
		getObjectFn: func(_ context.Context, input *awss3.GetObjectInput, _ ...func(*awss3.Options)) (*awss3.GetObjectOutput, error) {
			calls++
			if calls == 1 {
				return &awss3.GetObjectOutput{
					Body:          &failingReadCloser{payload: []byte("abc"), err: wantErr},
					ContentLength: new(int64(len(payload))),
					ContentRange:  new(fmt.Sprintf("bytes 0-5/%d", len(payload))),
				}, nil
			}
			start, end, err := parseRequestRange(aws.ToString(input.Range))
			if err != nil {
				return nil, err
			}
			return testRangeOutput(payload, start, end), nil
		},
	}
	manager := newTransferManager(client, transferOptions{partSize: 6, concurrency: 1, bodyAttempts: 2})
	reader, _, err := manager.openReader(context.Background(), "bucket", "key")
	if err != nil {
		t.Fatalf("openReader() error = %v", err)
	}
	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if string(got) != payload || calls != 2 {
		t.Fatalf("payload = %q calls = %d", got, calls)
	}
}

func TestTransferReaderCachesPermanentError(t *testing.T) {
	wantErr := errors.New("get failed")
	calls := 0
	client := &fakeTransferS3Client{
		headObjectFn: func(context.Context, *awss3.HeadObjectInput, ...func(*awss3.Options)) (*awss3.HeadObjectOutput, error) {
			return &awss3.HeadObjectOutput{ContentLength: new(int64(3)), ETag: new(`"etag"`)}, nil
		},
		getObjectFn: func(context.Context, *awss3.GetObjectInput, ...func(*awss3.Options)) (*awss3.GetObjectOutput, error) {
			calls++
			return nil, wantErr
		},
	}
	manager := newTransferManager(client, transferOptions{partSize: 3, concurrency: 1, bodyAttempts: 3})
	reader, _, err := manager.openReader(context.Background(), "bucket", "key")
	if err != nil {
		t.Fatalf("openReader() error = %v", err)
	}
	buffer := make([]byte, 3)
	if _, err := reader.Read(buffer); !errors.Is(err, wantErr) {
		t.Fatalf("first Read() error = %v, want %v", err, wantErr)
	}
	if _, err := reader.Read(buffer); !errors.Is(err, wantErr) {
		t.Fatalf("second Read() error = %v, want %v", err, wantErr)
	}
	if calls != 1 {
		t.Fatalf("GetObject() calls = %d, want 1", calls)
	}
}

func TestTransferReaderCloseInterruptsActiveBody(t *testing.T) {
	body := newBlockingReadCloser()
	client := &fakeTransferS3Client{
		headObjectFn: func(context.Context, *awss3.HeadObjectInput, ...func(*awss3.Options)) (*awss3.HeadObjectOutput, error) {
			return &awss3.HeadObjectOutput{ContentLength: new(int64(3)), ETag: new(`"etag"`)}, nil
		},
		getObjectFn: func(context.Context, *awss3.GetObjectInput, ...func(*awss3.Options)) (*awss3.GetObjectOutput, error) {
			return &awss3.GetObjectOutput{
				Body:          body,
				ContentLength: new(int64(3)),
				ContentRange:  new("bytes 0-2/3"),
			}, nil
		},
	}
	manager := newTransferManager(client, transferOptions{partSize: 3, concurrency: 1, bodyAttempts: 1})
	reader, _, err := manager.openReader(context.Background(), "bucket", "key")
	if err != nil {
		t.Fatalf("openReader() error = %v", err)
	}

	select {
	case <-body.started:
	case <-time.After(2 * time.Second):
		t.Fatal("response body read did not start")
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	select {
	case <-body.closed:
	default:
		t.Fatal("active response body was not closed")
	}
}

func TestTransferReaderCloseDiscardsBufferedPart(t *testing.T) {
	payload := "abcdef"
	client := &fakeTransferS3Client{
		headObjectFn: func(context.Context, *awss3.HeadObjectInput, ...func(*awss3.Options)) (*awss3.HeadObjectOutput, error) {
			return &awss3.HeadObjectOutput{ContentLength: new(int64(len(payload))), ETag: new(`"etag"`)}, nil
		},
		getObjectFn: func(_ context.Context, input *awss3.GetObjectInput, _ ...func(*awss3.Options)) (*awss3.GetObjectOutput, error) {
			start, end, err := parseRequestRange(aws.ToString(input.Range))
			if err != nil {
				return nil, err
			}
			return testRangeOutput(payload, start, end), nil
		},
	}
	reader, _, err := newTransferManager(client, transferOptions{partSize: 6, concurrency: 1}).openReader(context.Background(), "bucket", "key")
	if err != nil {
		t.Fatalf("openReader() error = %v", err)
	}
	buffer := make([]byte, 2)
	if n, err := reader.Read(buffer); err != nil || n != len(buffer) {
		t.Fatalf("Read() = (%d, %v), want (2, nil)", n, err)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if n, err := reader.Read(buffer); n != 0 || !errors.Is(err, errTransferReaderClosed) {
		t.Fatalf("Read() after Close = (%d, %v), want closed error", n, err)
	}
}

func TestTransferReaderRejectsMissingSnapshotFence(t *testing.T) {
	client := &fakeTransferS3Client{
		headObjectFn: func(context.Context, *awss3.HeadObjectInput, ...func(*awss3.Options)) (*awss3.HeadObjectOutput, error) {
			return &awss3.HeadObjectOutput{ContentLength: new(int64(1))}, nil
		},
	}
	manager := newTestTransferManager(client, 1, 1)
	if _, _, err := manager.openReader(context.Background(), "bucket", "key"); err == nil || !strings.Contains(err.Error(), "VersionId and ETag") {
		t.Fatalf("openReader() error = %v, want missing fence", err)
	}
}

func TestTransferReaderEmptyObjectNeedsNoGet(t *testing.T) {
	client := &fakeTransferS3Client{
		headObjectFn: func(context.Context, *awss3.HeadObjectInput, ...func(*awss3.Options)) (*awss3.HeadObjectOutput, error) {
			return &awss3.HeadObjectOutput{ContentLength: new(int64(0))}, nil
		},
	}
	manager := newTestTransferManager(client, 1, 1)
	reader, metadata, err := manager.openReader(context.Background(), "bucket", "key")
	if err != nil {
		t.Fatalf("openReader() error = %v", err)
	}
	if metadata.Size != 0 {
		t.Fatalf("metadata.Size = %d, want 0", metadata.Size)
	}
	if body, err := io.ReadAll(reader); err != nil || len(body) != 0 {
		t.Fatalf("ReadAll() = (%q, %v), want empty", body, err)
	}
}

func parseRequestRange(value string) (int64, int64, error) {
	var start, end int64
	if _, err := fmt.Sscanf(value, "bytes=%d-%d", &start, &end); err != nil {
		return 0, 0, fmt.Errorf("parse request range %q: %w", value, err)
	}
	return start, end, nil
}

func testRangeOutput(payload string, start, end int64) *awss3.GetObjectOutput {
	chunk := payload[start : end+1]
	return &awss3.GetObjectOutput{
		Body:          io.NopCloser(strings.NewReader(chunk)),
		ContentLength: new(int64(len(chunk))),
		ContentRange:  new(fmt.Sprintf("bytes %d-%d/%d", start, end, len(payload))),
	}
}

type failingReadCloser struct {
	payload []byte
	err     error
	done    bool
}

func (r *failingReadCloser) Read(buffer []byte) (int, error) {
	if r.done {
		return 0, r.err
	}
	r.done = true
	return copy(buffer, r.payload), r.err
}

func (*failingReadCloser) Close() error { return nil }

type blockingReadCloser struct {
	started   chan struct{}
	closed    chan struct{}
	startOnce sync.Once
	closeOnce sync.Once
}

func newBlockingReadCloser() *blockingReadCloser {
	return &blockingReadCloser{started: make(chan struct{}), closed: make(chan struct{})}
}

func (r *blockingReadCloser) Read([]byte) (int, error) {
	r.startOnce.Do(func() { close(r.started) })
	<-r.closed
	return 0, errTransferReaderClosed
}

func (r *blockingReadCloser) Close() error {
	r.closeOnce.Do(func() { close(r.closed) })
	return nil
}
