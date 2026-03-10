package s3

import (
	"context"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	tmtypes "github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager/types"
	"github.com/islishude/gotgz/internal/locator"
)

// TestStoreRejectsNonS3Refs verifies that S3-backed methods fail fast when
// handed a non-S3 locator reference.
func TestStoreRejectsNonS3Refs(t *testing.T) {
	store := &Store{}
	ref := locator.Ref{Kind: locator.KindLocal, Raw: "archive.tar", Path: "archive.tar"}

	if _, _, err := store.OpenReader(context.Background(), ref); err == nil {
		t.Fatalf("OpenReader() error = nil, want non-nil")
	}
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
			if got := derefString(in.SSEKMSKeyID); got != tt.wantKMS {
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

	if got := mergeMetadata(nil, nil); got != nil {
		t.Fatalf("mergeMetadata(nil, nil) = %#v, want nil", got)
	}

	base := map[string]string{"owner": "platform", "team": "storage"}
	overlay := map[string]string{"team": "archive", "trace": "enabled"}
	want := map[string]string{"owner": "platform", "team": "archive", "trace": "enabled"}
	if got := mergeMetadata(base, overlay); !reflect.DeepEqual(got, want) {
		t.Fatalf("mergeMetadata() = %#v, want %#v", got, want)
	}

	if !reflect.DeepEqual(base, map[string]string{"owner": "platform", "team": "storage"}) {
		t.Fatalf("mergeMetadata() mutated base map: %#v", base)
	}
}

// derefString returns the pointed-to string or an empty string when nil.
func derefString(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}
