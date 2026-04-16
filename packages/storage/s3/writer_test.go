package s3

import (
	"context"
	"errors"
	"io"
	"net/url"
	"reflect"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	tmtypes "github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager/types"
	"github.com/islishude/gotgz/packages/archiveutil"
	"github.com/islishude/gotgz/packages/locator"
)

func TestWriterMethodsRejectNonS3Refs(t *testing.T) {
	store := &Store{}
	ref := locator.Ref{Kind: locator.KindLocal, Raw: "archive.tar", Path: "archive.tar"}

	if _, err := store.OpenWriter(context.Background(), ref, nil); err == nil {
		t.Fatalf("OpenWriter() error = nil, want non-nil")
	}
	if err := store.UploadStream(context.Background(), ref, strings.NewReader("payload"), nil); err == nil {
		t.Fatalf("UploadStream() error = nil, want non-nil")
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
