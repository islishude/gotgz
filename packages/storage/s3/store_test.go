package s3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/url"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	tmtypes "github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager/types"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/islishude/gotgz/packages/archiveutil"
	"github.com/islishude/gotgz/packages/locator"
)

var _ transfermanager.S3APIClient = (*fakeTransferS3Client)(nil)

// fakeTransferS3Client implements the transfer-manager S3 interface with
// overridable hooks for the download-focused tests in this package.
type fakeTransferS3Client struct {
	headObjectFn func(context.Context, *awss3.HeadObjectInput, ...func(*awss3.Options)) (*awss3.HeadObjectOutput, error)
	getObjectFn  func(context.Context, *awss3.GetObjectInput, ...func(*awss3.Options)) (*awss3.GetObjectOutput, error)
}

// PutObject rejects unexpected upload calls in download tests.
func (c *fakeTransferS3Client) PutObject(context.Context, *awss3.PutObjectInput, ...func(*awss3.Options)) (*awss3.PutObjectOutput, error) {
	return nil, fmt.Errorf("unexpected PutObject call")
}

// UploadPart rejects unexpected multipart-upload calls in download tests.
func (c *fakeTransferS3Client) UploadPart(context.Context, *awss3.UploadPartInput, ...func(*awss3.Options)) (*awss3.UploadPartOutput, error) {
	return nil, fmt.Errorf("unexpected UploadPart call")
}

// CreateMultipartUpload rejects unexpected multipart-upload calls in download tests.
func (c *fakeTransferS3Client) CreateMultipartUpload(context.Context, *awss3.CreateMultipartUploadInput, ...func(*awss3.Options)) (*awss3.CreateMultipartUploadOutput, error) {
	return nil, fmt.Errorf("unexpected CreateMultipartUpload call")
}

// CompleteMultipartUpload rejects unexpected multipart-upload calls in download tests.
func (c *fakeTransferS3Client) CompleteMultipartUpload(context.Context, *awss3.CompleteMultipartUploadInput, ...func(*awss3.Options)) (*awss3.CompleteMultipartUploadOutput, error) {
	return nil, fmt.Errorf("unexpected CompleteMultipartUpload call")
}

// AbortMultipartUpload rejects unexpected multipart-upload calls in download tests.
func (c *fakeTransferS3Client) AbortMultipartUpload(context.Context, *awss3.AbortMultipartUploadInput, ...func(*awss3.Options)) (*awss3.AbortMultipartUploadOutput, error) {
	return nil, fmt.Errorf("unexpected AbortMultipartUpload call")
}

// GetObject delegates to the test hook when provided.
func (c *fakeTransferS3Client) GetObject(ctx context.Context, in *awss3.GetObjectInput, optFns ...func(*awss3.Options)) (*awss3.GetObjectOutput, error) {
	if c.getObjectFn == nil {
		return nil, fmt.Errorf("unexpected GetObject call")
	}
	return c.getObjectFn(ctx, in, optFns...)
}

// HeadObject delegates to the test hook when provided.
func (c *fakeTransferS3Client) HeadObject(ctx context.Context, in *awss3.HeadObjectInput, optFns ...func(*awss3.Options)) (*awss3.HeadObjectOutput, error) {
	if c.headObjectFn == nil {
		return nil, fmt.Errorf("unexpected HeadObject call")
	}
	return c.headObjectFn(ctx, in, optFns...)
}

// ListObjectsV2 rejects unexpected listing calls in download tests.
func (c *fakeTransferS3Client) ListObjectsV2(context.Context, *awss3.ListObjectsV2Input, ...func(*awss3.Options)) (*awss3.ListObjectsV2Output, error) {
	return nil, fmt.Errorf("unexpected ListObjectsV2 call")
}

// closeTrackingReader records Close calls so wrapper tests can verify
// idempotent cleanup behavior.
type closeTrackingReader struct {
	reader     io.Reader
	closeErr   error
	closeCalls int
}

// Read forwards reads to the wrapped reader.
func (r *closeTrackingReader) Read(p []byte) (int, error) {
	return r.reader.Read(p)
}

// Close records the call and returns the configured error.
func (r *closeTrackingReader) Close() error {
	r.closeCalls++
	return r.closeErr
}

// expectedByteRanges returns the transfer-manager range requests expected for a
// given payload length and part size.
func expectedByteRanges(total int, partSize int64) []string {
	ranges := make([]string, 0)
	for start := int64(0); start < int64(total); start += partSize {
		end := start + partSize - 1
		if end >= int64(total) {
			end = int64(total) - 1
		}
		ranges = append(ranges, fmt.Sprintf("bytes=%d-%d", start, end))
	}
	return ranges
}

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

// TestStoreRejectsNonS3Refs verifies that S3-backed methods fail fast when
// handed a non-S3 locator reference.
func TestStoreRejectsNonS3Refs(t *testing.T) {
	store := &Store{}
	ref := locator.Ref{Kind: locator.KindLocal, Raw: "archive.tar", Path: "archive.tar"}

	if _, _, err := store.OpenReader(context.Background(), ref); err == nil {
		t.Fatalf("OpenReader() error = nil, want non-nil")
	}
	if _, err := store.OpenRangeReader(context.Background(), ref, 0, 1); err == nil {
		t.Fatalf("OpenRangeReader() error = nil, want non-nil")
	}
	if _, err := store.OpenWriter(context.Background(), ref, nil); err == nil {
		t.Fatalf("OpenWriter() error = nil, want non-nil")
	}
	if err := store.UploadStream(context.Background(), ref, strings.NewReader("payload"), nil); err == nil {
		t.Fatalf("UploadStream() error = nil, want non-nil")
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

// TestStoreApplyEncryption verifies that upload encryption settings are mapped
// onto transfer-manager input fields.
func TestStoreApplyEncryption(t *testing.T) {
	tests := []struct {
		name    string
		store   Store
		wantSSE tmtypes.ServerSideEncryption
		wantKMS string
	}{
		{
			name:    "default aes256",
			store:   Store{settings: Settings{SSE: ""}},
			wantSSE: tmtypes.ServerSideEncryptionAes256,
		},
		{
			name:    "kms with key id",
			store:   Store{settings: Settings{SSE: "sse-kms", SSEKMSKeyID: "kms-key-id"}},
			wantSSE: tmtypes.ServerSideEncryptionAwsKms,
			wantKMS: "kms-key-id",
		},
		{
			name:    "none leaves fields unset",
			store:   Store{settings: Settings{SSE: "none"}},
			wantSSE: "",
		},
		{
			name:    "unknown falls back to aes256",
			store:   Store{settings: Settings{SSE: "unexpected"}},
			wantSSE: tmtypes.ServerSideEncryptionAes256,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := &transfermanager.UploadObjectInput{}
			tt.store.applyEncryption(in)

			if in.ServerSideEncryption != tt.wantSSE {
				t.Fatalf("applyEncryption() SSE = %q, want %q", in.ServerSideEncryption, tt.wantSSE)
			}
			if got := aws.ToString(in.SSEKMSKeyID); got != tt.wantKMS {
				t.Fatalf("applyEncryption() SSEKMSKeyID = %q, want %q", got, tt.wantKMS)
			}
		})
	}
}

// TestUploadWriterWriteAndClose verifies that uploadWriter forwards bytes to
// the pipe writer and returns a successful close when the async upload reports
// no error.
func TestUploadWriterWriteAndClose(t *testing.T) {
	pr, pw := io.Pipe()
	errCh := make(chan error)
	close(errCh)
	writer := &uploadWriter{pw: pw, errCh: errCh}

	done := make(chan []byte, 1)
	go func() {
		data, err := io.ReadAll(pr)
		if err != nil {
			done <- []byte(err.Error())
			return
		}
		done <- data
	}()

	if n, err := writer.Write([]byte("payload")); err != nil || n != len("payload") {
		t.Fatalf("Write() = (%d, %v), want (%d, nil)", n, err, len("payload"))
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	got := <-done
	if string(got) != "payload" {
		t.Fatalf("pipe payload = %q, want %q", got, "payload")
	}
	if err := pr.Close(); err != nil {
		t.Fatalf("PipeReader.Close() error = %v", err)
	}
}

// TestUploadWriterCloseReturnsAsyncError verifies that the background upload
// error is surfaced after the pipe closes.
func TestUploadWriterCloseReturnsAsyncError(t *testing.T) {
	pr, pw := io.Pipe()
	if err := pr.Close(); err != nil {
		t.Fatalf("PipeReader.Close() error = %v", err)
	}

	wantErr := errors.New("upload failed")
	errCh := make(chan error, 1)
	errCh <- wantErr
	close(errCh)

	writer := &uploadWriter{pw: pw, errCh: errCh}
	if err := writer.Close(); !errors.Is(err, wantErr) {
		t.Fatalf("Close() error = %v, want %v", err, wantErr)
	}
}

// TestUploadGoroutinePropagatesErrorThroughPipe verifies that when the
// background upload fails, ongoing writes to the PipeWriter see the real
// upload error (not the generic io.ErrClosedPipe).
func TestUploadGoroutinePropagatesErrorThroughPipe(t *testing.T) {
	wantErr := errors.New("s3 upload failed")
	pr, pw := io.Pipe()
	errCh := make(chan error, 1)

	// Simulate the goroutine in OpenWriter: upload reads one chunk then
	// fails, closing the reader with the real error.
	go func() {
		buf := make([]byte, 256)
		_, _ = pr.Read(buf)

		_ = pr.CloseWithError(wantErr)
		errCh <- wantErr
		close(errCh)
	}()

	writer := &uploadWriter{pw: pw, errCh: errCh}

	// First write delivers data to the goroutine (must fit in one Read).
	if _, err := writer.Write([]byte("data")); err != nil {
		t.Fatalf("first Write() error = %v", err)
	}

	// Subsequent writes must see the real upload error, not io.ErrClosedPipe.
	for range 10 {
		_, err := writer.Write([]byte("more data"))
		if err != nil {
			if errors.Is(err, io.ErrClosedPipe) {
				t.Fatalf("Write() returned io.ErrClosedPipe; want the real upload error %q", wantErr)
			}
			if !errors.Is(err, wantErr) {
				t.Fatalf("Write() error = %v, want %v", err, wantErr)
			}
			return
		}
	}
	t.Fatal("expected a write error, but all writes succeeded")
}

// TestIntFromEnv verifies that integer environment settings are parsed only
// when present and valid.
func TestIntFromEnv(t *testing.T) {
	t.Setenv("GOTGZ_TEST_INT", " 42 ")
	if got, ok := intFromEnv("GOTGZ_TEST_INT"); !ok || got != 42 {
		t.Fatalf("intFromEnv(valid) = (%d, %t), want (42, true)", got, ok)
	}

	t.Setenv("GOTGZ_TEST_INT", "bad")
	if got, ok := intFromEnv("GOTGZ_TEST_INT"); ok || got != 0 {
		t.Fatalf("intFromEnv(invalid) = (%d, %t), want (0, false)", got, ok)
	}

	t.Setenv("GOTGZ_TEST_INT", "")
	if got, ok := intFromEnv("GOTGZ_TEST_INT"); ok || got != 0 {
		t.Fatalf("intFromEnv(empty) = (%d, %t), want (0, false)", got, ok)
	}
}

// TestInt64FromEnv verifies that int64 environment settings are parsed only
// when present and valid.
func TestInt64FromEnv(t *testing.T) {
	t.Setenv("GOTGZ_TEST_INT64", " 4096 ")
	if got, ok := int64FromEnv("GOTGZ_TEST_INT64"); !ok || got != 4096 {
		t.Fatalf("int64FromEnv(valid) = (%d, %t), want (4096, true)", got, ok)
	}

	t.Setenv("GOTGZ_TEST_INT64", "bad")
	if got, ok := int64FromEnv("GOTGZ_TEST_INT64"); ok || got != 0 {
		t.Fatalf("int64FromEnv(invalid) = (%d, %t), want (0, false)", got, ok)
	}

	t.Setenv("GOTGZ_TEST_INT64", "")
	if got, ok := int64FromEnv("GOTGZ_TEST_INT64"); ok || got != 0 {
		t.Fatalf("int64FromEnv(empty) = (%d, %t), want (0, false)", got, ok)
	}
}

// TestDefaultStringAndMergeMetadata verifies that empty strings fall back to
// defaults and overlay metadata overrides base keys.
func TestDefaultStringAndMergeMetadata(t *testing.T) {
	if got := defaultString(" value ", "fallback"); got != " value " {
		t.Fatalf("defaultString(non-empty) = %q, want %q", got, " value ")
	}
	if got := defaultString("   ", "fallback"); got != "fallback" {
		t.Fatalf("defaultString(blank) = %q, want %q", got, "fallback")
	}

	if got := archiveutil.MergeMetadata(nil, nil); got != nil {
		t.Fatalf("archiveutil.MergeMetadata(nil, nil) = %#v, want nil", got)
	}

	base := map[string]string{"owner": "platform", "team": "storage"}
	overlay := map[string]string{"team": "archive", "trace": "enabled"}
	want := map[string]string{"owner": "platform", "team": "archive", "trace": "enabled"}
	if got := archiveutil.MergeMetadata(base, overlay); !reflect.DeepEqual(got, want) {
		t.Fatalf("archiveutil.MergeMetadata() = %#v, want %#v", got, want)
	}

	if !reflect.DeepEqual(base, map[string]string{"owner": "platform", "team": "storage"}) {
		t.Fatalf("archiveutil.MergeMetadata() mutated base map: %#v", base)
	}
}

// TestEncodeObjectTagging verifies that user-provided tags are encoded
// without adding any implicit tags.
func TestEncodeObjectTagging(t *testing.T) {
	got := encodeObjectTagging(map[string]string{
		"team":             "archive ops",
		"gotgz-created-at": "user-provided",
		"component":        "s3/upload",
	})

	values, err := url.ParseQuery(got)
	if err != nil {
		t.Fatalf("ParseQuery() error = %v", err)
	}
	if values.Get("component") != "s3/upload" {
		t.Fatalf("component = %q", values.Get("component"))
	}
	if values.Get("team") != "archive ops" {
		t.Fatalf("team = %q", values.Get("team"))
	}
	if values.Get("gotgz-created-at") != "user-provided" {
		t.Fatalf("gotgz-created-at = %q", values.Get("gotgz-created-at"))
	}
}

// TestEncodeObjectTaggingWithoutUserTags verifies that no Tagging header is
// emitted when callers do not provide any tags.
func TestEncodeObjectTaggingWithoutUserTags(t *testing.T) {
	got := encodeObjectTagging(nil)
	if got != "" {
		t.Fatalf("encodeObjectTagging(nil) = %q", got)
	}
}

// TestEncodeObjectTaggingRoundTrip verifies that special characters survive the
// URL-query encoding required by the S3 tagging header.
func TestEncodeObjectTaggingRoundTrip(t *testing.T) {
	got := encodeObjectTagging(map[string]string{"trace": "a=b&c"})

	values, err := url.ParseQuery(got)
	if err != nil {
		t.Fatalf("ParseQuery() error = %v", err)
	}
	if values.Get("trace") != "a=b&c" {
		t.Fatalf("trace = %q", values.Get("trace"))
	}
	if _, ok := values["gotgz-created-at"]; ok {
		t.Fatalf("unexpected implicit gotgz-created-at tag in %#v", values)
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
