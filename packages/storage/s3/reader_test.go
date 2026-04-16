package s3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/islishude/gotgz/packages/locator"
)

func TestStatRejectsNonS3Ref(t *testing.T) {
	s := &Store{}
	_, err := s.Stat(context.Background(), locator.Ref{Kind: locator.KindLocal, Raw: "local.tar"})
	if err == nil {
		t.Fatalf("expected error for non-s3 ref")
	}
}

// TestOpenReaderUsesTransferManagerRanges verifies that OpenReader uses
// transfer manager range downloads and preserves metadata for the caller.
func TestOpenReaderUsesTransferManagerRanges(t *testing.T) {
	ref := locator.Ref{Kind: locator.KindS3, Raw: "s3://bucket/object", Bucket: "bucket", Key: "object"}
	payload := "hello-world!"
	contentType := "application/octet-stream"

	var (
		mu            sync.Mutex
		headCalls     int
		gotRanges     []string
		sawPartNumber bool
	)

	client := &fakeTransferS3Client{
		headObjectFn: func(_ context.Context, in *awss3.HeadObjectInput, _ ...func(*awss3.Options)) (*awss3.HeadObjectOutput, error) {
			mu.Lock()
			headCalls++
			mu.Unlock()

			if got := aws.ToString(in.Bucket); got != ref.Bucket {
				return nil, fmt.Errorf("HeadObject() bucket = %q, want %q", got, ref.Bucket)
			}
			if got := aws.ToString(in.Key); got != ref.Key {
				return nil, fmt.Errorf("HeadObject() key = %q, want %q", got, ref.Key)
			}
			return &awss3.HeadObjectOutput{
				ContentLength: aws.Int64(int64(len(payload))),
				ContentType:   aws.String(contentType),
				ETag:          aws.String("etag"),
			}, nil
		},
		getObjectFn: func(_ context.Context, in *awss3.GetObjectInput, _ ...func(*awss3.Options)) (*awss3.GetObjectOutput, error) {
			if in.Range == nil {
				return nil, fmt.Errorf("GetObject() range is nil")
			}
			if in.PartNumber != nil {
				mu.Lock()
				sawPartNumber = true
				mu.Unlock()
				return nil, fmt.Errorf("GetObject() unexpectedly used PartNumber %d", aws.ToInt32(in.PartNumber))
			}

			var start, end int64
			if _, err := fmt.Sscanf(aws.ToString(in.Range), "bytes=%d-%d", &start, &end); err != nil {
				return nil, fmt.Errorf("parse range %q: %w", aws.ToString(in.Range), err)
			}
			chunk := payload[start : end+1]

			mu.Lock()
			gotRanges = append(gotRanges, aws.ToString(in.Range))
			mu.Unlock()

			return &awss3.GetObjectOutput{
				Body:          io.NopCloser(strings.NewReader(chunk)),
				ContentLength: aws.Int64(int64(len(chunk))),
				ContentRange:  aws.String(fmt.Sprintf("bytes %d-%d/%d", start, end, len(payload))),
			}, nil
		},
	}

	store := &Store{
		tm: transfermanager.New(client, func(o *transfermanager.Options) {
			o.PartSizeBytes = 5
			o.Concurrency = 2
		}),
	}

	rc, meta, err := store.OpenReader(context.Background(), ref)
	if err != nil {
		t.Fatalf("OpenReader() error = %v", err)
	}

	mu.Lock()
	gotHeadCalls := headCalls
	mu.Unlock()
	if gotHeadCalls != 1 {
		t.Fatalf("HeadObject() calls = %d, want 1", gotHeadCalls)
	}

	body, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if err := rc.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	if got := string(body); got != payload {
		t.Fatalf("payload = %q, want %q", got, payload)
	}
	if meta.Size != int64(len(payload)) {
		t.Fatalf("metadata size = %d, want %d", meta.Size, len(payload))
	}
	if meta.ContentType != contentType {
		t.Fatalf("metadata content type = %q, want %q", meta.ContentType, contentType)
	}

	wantRanges := expectedByteRanges(len(payload), 5)
	mu.Lock()
	gotRangesCopy := append([]string(nil), gotRanges...)
	sawPartNumberCopy := sawPartNumber
	sort.Strings(wantRanges)
	mu.Unlock()
	sort.Strings(gotRangesCopy)
	if sawPartNumberCopy {
		t.Fatal("GetObject() unexpectedly used multipart part downloads")
	}
	if !reflect.DeepEqual(gotRangesCopy, wantRanges) {
		t.Fatalf("range requests = %#v, want %#v", gotRangesCopy, wantRanges)
	}
}

// TestOpenReaderReturnsHeadObjectError verifies that setup failures from the
// transfer-manager HeadObject call surface directly from OpenReader.
func TestOpenReaderReturnsHeadObjectError(t *testing.T) {
	wantErr := errors.New("head failed")
	store := &Store{
		tm: transfermanager.New(&fakeTransferS3Client{
			headObjectFn: func(context.Context, *awss3.HeadObjectInput, ...func(*awss3.Options)) (*awss3.HeadObjectOutput, error) {
				return nil, wantErr
			},
		}),
	}

	_, _, err := store.OpenReader(context.Background(), locator.Ref{
		Kind:   locator.KindS3,
		Raw:    "s3://bucket/object",
		Bucket: "bucket",
		Key:    "object",
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("OpenReader() error = %v, want %v", err, wantErr)
	}
}

// TestOpenReaderPropagatesGetObjectError verifies that transfer-manager read
// failures from ranged GetObject calls surface while the caller reads.
func TestOpenReaderPropagatesGetObjectError(t *testing.T) {
	wantErr := errors.New("download failed")
	store := &Store{
		tm: transfermanager.New(&fakeTransferS3Client{
			headObjectFn: func(context.Context, *awss3.HeadObjectInput, ...func(*awss3.Options)) (*awss3.HeadObjectOutput, error) {
				return &awss3.HeadObjectOutput{
					ContentLength: aws.Int64(6),
					ETag:          aws.String("etag"),
				}, nil
			},
			getObjectFn: func(context.Context, *awss3.GetObjectInput, ...func(*awss3.Options)) (*awss3.GetObjectOutput, error) {
				return nil, wantErr
			},
		}, func(o *transfermanager.Options) {
			o.PartSizeBytes = 3
			o.Concurrency = 2
		}),
	}

	rc, meta, err := store.OpenReader(context.Background(), locator.Ref{
		Kind:   locator.KindS3,
		Raw:    "s3://bucket/object",
		Bucket: "bucket",
		Key:    "object",
	})
	if err != nil {
		t.Fatalf("OpenReader() error = %v", err)
	}
	if meta.Size != 6 {
		t.Fatalf("metadata size = %d, want 6", meta.Size)
	}

	_, err = io.ReadAll(rc)
	if !errors.Is(err, wantErr) {
		t.Fatalf("ReadAll() error = %v, want %v", err, wantErr)
	}
	if err := rc.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

// TestOpenReaderHandlesShortLenLargeCapBuffers verifies that callers can read
// with a slice whose backing array has extra capacity without triggering the
// transfer-manager concurrent-reader panic.
func TestOpenReaderHandlesShortLenLargeCapBuffers(t *testing.T) {
	ref := locator.Ref{Kind: locator.KindS3, Raw: "s3://bucket/object", Bucket: "bucket", Key: "object"}
	payload := "hello-world!"

	store := &Store{
		tm: transfermanager.New(&fakeTransferS3Client{
			headObjectFn: func(_ context.Context, in *awss3.HeadObjectInput, _ ...func(*awss3.Options)) (*awss3.HeadObjectOutput, error) {
				if got := aws.ToString(in.Bucket); got != ref.Bucket {
					return nil, fmt.Errorf("HeadObject() bucket = %q, want %q", got, ref.Bucket)
				}
				if got := aws.ToString(in.Key); got != ref.Key {
					return nil, fmt.Errorf("HeadObject() key = %q, want %q", got, ref.Key)
				}
				return &awss3.HeadObjectOutput{
					ContentLength: aws.Int64(int64(len(payload))),
					ETag:          aws.String("etag"),
				}, nil
			},
			getObjectFn: func(_ context.Context, in *awss3.GetObjectInput, _ ...func(*awss3.Options)) (*awss3.GetObjectOutput, error) {
				if in.Range == nil {
					return nil, fmt.Errorf("GetObject() range is nil")
				}

				var start, end int64
				if _, err := fmt.Sscanf(aws.ToString(in.Range), "bytes=%d-%d", &start, &end); err != nil {
					return nil, fmt.Errorf("parse range %q: %w", aws.ToString(in.Range), err)
				}
				chunk := payload[start : end+1]

				return &awss3.GetObjectOutput{
					Body:          io.NopCloser(strings.NewReader(chunk)),
					ContentLength: aws.Int64(int64(len(chunk))),
					ContentRange:  aws.String(fmt.Sprintf("bytes %d-%d/%d", start, end, len(payload))),
				}, nil
			},
		}, func(o *transfermanager.Options) {
			o.PartSizeBytes = 5
			o.Concurrency = 2
		}),
	}

	rc, meta, err := store.OpenReader(context.Background(), ref)
	if err != nil {
		t.Fatalf("OpenReader() error = %v", err)
	}
	if meta.Size != int64(len(payload)) {
		t.Fatalf("metadata size = %d, want %d", meta.Size, len(payload))
	}

	buf := make([]byte, len(payload))
	n, err := rc.Read(buf[:4])
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if got := string(buf[:n]); got != payload[:4] {
		t.Fatalf("first read = %q, want %q", got, payload[:4])
	}

	rest, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if got := string(buf[:n]) + string(rest); got != payload {
		t.Fatalf("payload = %q, want %q", got, payload)
	}
	if err := rc.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestReaderMethodsRejectNonS3Refs(t *testing.T) {
	store := &Store{}
	ref := locator.Ref{Kind: locator.KindLocal, Raw: "archive.tar", Path: "archive.tar"}

	if _, _, err := store.OpenReader(context.Background(), ref); err == nil {
		t.Fatalf("OpenReader() error = nil, want non-nil")
	}
	if _, err := store.OpenRangeReader(context.Background(), ref, 0, 1); err == nil {
		t.Fatalf("OpenRangeReader() error = nil, want non-nil")
	}
	if _, err := store.Stat(context.Background(), ref); err == nil {
		t.Fatalf("Stat() error = nil, want non-nil")
	}
}

// TestOpenRangeReaderRejectsOverflow verifies that byte range calculation
// fails before constructing an invalid Range header when the end offset would
// overflow int64.
func TestOpenRangeReaderRejectsOverflow(t *testing.T) {
	store := &Store{}
	ref := locator.Ref{Kind: locator.KindS3, Raw: "s3://bucket/object", Bucket: "bucket", Key: "object"}

	_, err := store.OpenRangeReader(context.Background(), ref, math.MaxInt64, 2)
	if err == nil {
		t.Fatal("OpenRangeReader() error = nil, want non-nil")
	}
	if got := err.Error(); got != "range end overflows int64 for offset 9223372036854775807 and length 2" {
		t.Fatalf("OpenRangeReader() error = %q, want overflow error", got)
	}
}

// TestDownloadReadCloserCloseIsIdempotent verifies that Close only cancels and
// closes the wrapped reader once, even when the caller closes multiple times.
func TestDownloadReadCloserCloseIsIdempotent(t *testing.T) {
	wantErr := errors.New("close failed")
	reader := &closeTrackingReader{
		reader:   strings.NewReader("payload"),
		closeErr: wantErr,
	}

	cancelCalls := 0
	rc := newDownloadReadCloser(reader, func() {
		cancelCalls++
	})

	if err := rc.Close(); !errors.Is(err, wantErr) {
		t.Fatalf("first Close() error = %v, want %v", err, wantErr)
	}
	if err := rc.Close(); !errors.Is(err, wantErr) {
		t.Fatalf("second Close() error = %v, want %v", err, wantErr)
	}
	if cancelCalls != 1 {
		t.Fatalf("cancel calls = %d, want 1", cancelCalls)
	}
	if reader.closeCalls != 1 {
		t.Fatalf("reader close calls = %d, want 1", reader.closeCalls)
	}
}

// terminalErrorReader returns one terminal error and records how many times it
// was asked to read.
type terminalErrorReader struct {
	err       error
	readCalls int
}

// Read returns the configured error and increments the call count.
func (r *terminalErrorReader) Read([]byte) (int, error) {
	r.readCalls++
	return 0, r.err
}

// TestDownloadReadCloserCachesTerminalReadError verifies that callers who read
// again after a terminal read failure get the cached error without re-entering
// the wrapped reader.
func TestDownloadReadCloserCachesTerminalReadError(t *testing.T) {
	wantErr := errors.New("download failed")
	reader := &terminalErrorReader{err: wantErr}
	rc := newDownloadReadCloser(reader, nil)
	buf := make([]byte, 8)

	if _, err := rc.Read(buf); !errors.Is(err, wantErr) {
		t.Fatalf("first Read() error = %v, want %v", err, wantErr)
	}
	if _, err := rc.Read(buf); !errors.Is(err, wantErr) {
		t.Fatalf("second Read() error = %v, want %v", err, wantErr)
	}
	if reader.readCalls != 1 {
		t.Fatalf("wrapped reader read calls = %d, want 1", reader.readCalls)
	}
}
