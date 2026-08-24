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
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/islishude/gotgz/packages/archiveutil"
	"github.com/islishude/gotgz/packages/locator"
)

func TestStatRejectsNonS3Ref(t *testing.T) {
	s := &Store{}
	_, err := s.Stat(context.Background(), locator.Ref{Kind: locator.KindLocal, Raw: "local.tar"})
	if err == nil {
		t.Fatalf("expected error for non-s3 ref")
	}
}

// TestOpenReaderUsesConcurrentRanges verifies that OpenReader uses bounded
// range downloads and preserves metadata for the caller.
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
		client:    client,
		transfers: newTestTransferManager(client, 5, 2),
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
	if meta.ETag != "etag" {
		t.Fatalf("metadata ETag = %q, want etag", meta.ETag)
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

// TestOpenReaderReturnsHeadObjectError verifies that setup failures surface
// directly from OpenReader.
func TestOpenReaderReturnsHeadObjectError(t *testing.T) {
	wantErr := errors.New("head failed")
	client := &fakeTransferS3Client{
		headObjectFn: func(context.Context, *awss3.HeadObjectInput, ...func(*awss3.Options)) (*awss3.HeadObjectOutput, error) {
			return nil, wantErr
		},
	}
	store := &Store{
		client:    client,
		transfers: newTestTransferManager(client, 5, 2),
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

// TestOpenReaderPropagatesGetObjectError verifies that ranged GetObject
// failures surface while the caller reads.
func TestOpenReaderPropagatesGetObjectError(t *testing.T) {
	wantErr := errors.New("download failed")
	client := &fakeTransferS3Client{
		headObjectFn: func(context.Context, *awss3.HeadObjectInput, ...func(*awss3.Options)) (*awss3.HeadObjectOutput, error) {
			return &awss3.HeadObjectOutput{
				ContentLength: new(int64(6)),
				ETag:          new("etag"),
			}, nil
		},
		getObjectFn: func(context.Context, *awss3.GetObjectInput, ...func(*awss3.Options)) (*awss3.GetObjectOutput, error) {
			return nil, wantErr
		},
	}
	store := &Store{
		client:    client,
		transfers: newTestTransferManager(client, 3, 2),
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
// with a slice whose backing array has extra capacity.
func TestOpenReaderHandlesShortLenLargeCapBuffers(t *testing.T) {
	ref := locator.Ref{Kind: locator.KindS3, Raw: "s3://bucket/object", Bucket: "bucket", Key: "object"}
	payload := "hello-world!"

	client := &fakeTransferS3Client{
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
	}
	store := &Store{
		client:    client,
		transfers: newTestTransferManager(client, 5, 2),
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

func TestOpenRangeReaderSnapshotUsesVersionOrETagFence(t *testing.T) {
	tests := []struct {
		name        string
		snapshot    archiveutil.Snapshot
		wantVersion string
		wantETag    string
	}{
		{name: "version wins", snapshot: archiveutil.Snapshot{VersionID: "version-1", ETag: `"etag"`}, wantVersion: "version-1"},
		{name: "etag fallback", snapshot: archiveutil.Snapshot{ETag: `"etag"`}, wantETag: `"etag"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &Store{client: &fakeTransferS3Client{getObjectFn: func(_ context.Context, input *awss3.GetObjectInput, _ ...func(*awss3.Options)) (*awss3.GetObjectOutput, error) {
				if got := aws.ToString(input.VersionId); got != tt.wantVersion {
					t.Fatalf("VersionId = %q, want %q", got, tt.wantVersion)
				}
				if got := aws.ToString(input.IfMatch); got != tt.wantETag {
					t.Fatalf("IfMatch = %q, want %q", got, tt.wantETag)
				}
				return &awss3.GetObjectOutput{Body: io.NopCloser(strings.NewReader("data"))}, nil
			}}}
			rc, err := store.OpenRangeReaderSnapshot(context.Background(), locator.Ref{Kind: locator.KindS3, Bucket: "bucket", Key: "key"}, 0, 4, tt.snapshot)
			if err != nil {
				t.Fatalf("OpenRangeReaderSnapshot() error = %v", err)
			}
			defer rc.Close() //nolint:errcheck
		})
	}
}
