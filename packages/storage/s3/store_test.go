package s3

import (
	"context"
	"errors"
	"io"
	"math"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	tmtypes "github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager/types"
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

// TestEncodeObjectTagging verifies that user-provided tags are encoded and
// that the built-in created-at tag always wins over user input.
func TestEncodeObjectTagging(t *testing.T) {
	createdAt := time.Date(2026, time.March, 11, 9, 30, 0, 0, time.UTC)
	got := encodeObjectTagging(map[string]string{
		"team":             "archive ops",
		"gotgz-created-at": "user-overridden",
		"component":        "s3/upload",
	}, createdAt)

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
	if values.Get(createdAtObjectTagKey) != createdAt.Format(time.RFC3339) {
		t.Fatalf("created-at = %q", values.Get(createdAtObjectTagKey))
	}
}

// TestEncodeObjectTaggingWithoutUserTags verifies that S3 writes still receive
// the built-in created-at tag when callers do not provide custom tags.
func TestEncodeObjectTaggingWithoutUserTags(t *testing.T) {
	createdAt := time.Date(2026, time.March, 11, 9, 30, 0, 0, time.UTC)
	got := encodeObjectTagging(nil, createdAt)
	if got != "gotgz-created-at=2026-03-11T09%3A30%3A00Z" {
		t.Fatalf("encodeObjectTagging(nil) = %q", got)
	}
}

// TestEncodeObjectTaggingRoundTrip verifies that special characters survive the
// URL-query encoding required by the S3 tagging header.
func TestEncodeObjectTaggingRoundTrip(t *testing.T) {
	createdAt := time.Date(2026, time.March, 11, 9, 30, 0, 0, time.UTC)
	got := encodeObjectTagging(map[string]string{"trace": "a=b&c"}, createdAt)

	values, err := url.ParseQuery(got)
	if err != nil {
		t.Fatalf("ParseQuery() error = %v", err)
	}
	if values.Get("trace") != "a=b&c" {
		t.Fatalf("trace = %q", values.Get("trace"))
	}
	if values.Get(createdAtObjectTagKey) != createdAt.Format(time.RFC3339) {
		t.Fatalf("created-at = %q", values.Get(createdAtObjectTagKey))
	}
}
