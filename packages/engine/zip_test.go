package engine

import (
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/islishude/gotgz/packages/locator"
)

// TestWithZipReaderRespectsContextDuringTempCopy verifies staging copy aborts
// promptly when context cancellation happens mid-stream.
func TestWithZipReaderRespectsContextDuringTempCopy(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	src := newBlockingChunkReader(2, 1024)
	ar := io.NopCloser(src)

	done := make(chan error, 1)
	go func() {
		_, err := (&Runner{}).withZipReader(ctx, locator.Ref{Kind: locator.KindStdio}, ar, archiveReaderInfo{}, nil, func(_ *zip.Reader) (int, error) {
			return 0, errors.New("unexpected zip callback invocation")
		})
		done <- err
	}()

	src.waitForStart(t)
	cancel()
	src.allowRead()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("withZipReader() err = %v, want %v", err, context.Canceled)
		}
	case <-time.After(1500 * time.Millisecond):
		t.Fatal("withZipReader() did not stop after context cancellation")
	}
}

func TestWithZipReaderRejectsKnownOversizeBeforeCopy(t *testing.T) {
	t.Setenv(zipStagingLimitEnv, "64")

	called := false
	_, err := (&Runner{}).withZipReader(
		context.Background(),
		locator.Ref{Kind: locator.KindStdio, Raw: "-"},
		io.NopCloser(strings.NewReader("ignored")),
		archiveReaderInfo{Size: 65, SizeKnown: true},
		nil,
		func(_ *zip.Reader) (int, error) {
			called = true
			return 0, nil
		},
	)
	if err == nil || !strings.Contains(err.Error(), "staging limit") {
		t.Fatalf("withZipReader() err = %v, want staging limit error", err)
	}
	if called {
		t.Fatal("zip callback should not run for oversized input")
	}
}

func TestWithZipReaderRejectsUnknownOversizeDuringCopy(t *testing.T) {
	t.Setenv(zipStagingLimitEnv, "64")

	payload := zipArchiveBytes(t, map[string]string{
		"file.txt": strings.Repeat("x", 256),
	})
	called := false
	_, err := (&Runner{}).withZipReader(
		context.Background(),
		locator.Ref{Kind: locator.KindStdio, Raw: "-"},
		io.NopCloser(bytes.NewReader(payload)),
		archiveReaderInfo{},
		nil,
		func(_ *zip.Reader) (int, error) {
			called = true
			return 0, nil
		},
	)
	if err == nil || !strings.Contains(err.Error(), "staging limit") {
		t.Fatalf("withZipReader() err = %v, want staging limit error", err)
	}
	if called {
		t.Fatal("zip callback should not run for oversized staged input")
	}
}

func TestWithZipReaderPreservesLocalZipParseErrorWhenStagingFallbackIsUsed(t *testing.T) {
	t.Setenv(zipStagingLimitEnv, "64")

	path := t.TempDir() + "/invalid.zip"
	payload := bytes.Repeat([]byte("not-a-zip"), 32)
	if err := os.WriteFile(path, payload, 0o644); err != nil {
		t.Fatalf("os.WriteFile() error = %v", err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("os.Open() error = %v", err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			t.Fatalf("f.Close() error = %v", err)
		}
	}()

	st, err := f.Stat()
	if err != nil {
		t.Fatalf("f.Stat() error = %v", err)
	}
	expectedReader, err := os.Open(path)
	if err != nil {
		t.Fatalf("os.Open() expectedReader error = %v", err)
	}
	defer func() {
		if err := expectedReader.Close(); err != nil {
			t.Fatalf("expectedReader.Close() error = %v", err)
		}
	}()
	if _, expectedErr := zip.NewReader(expectedReader, st.Size()); expectedErr == nil {
		t.Fatal("zip.NewReader() unexpectedly accepted invalid local zip")
	} else {
		called := false
		_, err = (&Runner{}).withZipReader(
			context.Background(),
			locator.Ref{Kind: locator.KindLocal, Raw: path, Path: path},
			f,
			archiveReaderInfo{Size: st.Size(), SizeKnown: true},
			nil,
			func(_ *zip.Reader) (int, error) {
				called = true
				return 0, nil
			},
		)
		if err == nil {
			t.Fatal("withZipReader() error = nil, want invalid zip error")
		}
		if called {
			t.Fatal("zip callback should not run for invalid local zip")
		}
		if strings.Contains(err.Error(), "staging limit") {
			t.Fatalf("withZipReader() err = %v, want original zip parse error", err)
		}
		if err.Error() != expectedErr.Error() {
			t.Fatalf("withZipReader() err = %v, want %v", err, expectedErr)
		}
	}
}

// TestWithZipReaderUsesRemoteRangesForKnownS3Zip verifies that known-size S3
// zip inputs use remote range reads instead of staging the archive stream.
func TestWithZipReaderUsesRemoteRangesForKnownS3Zip(t *testing.T) {
	payload := zipArchiveBytes(t, map[string]string{
		"file.txt": "payload",
	})
	stream := &trackingReadCloser{Reader: strings.NewReader("unused-stream")}
	rangeCalls := 0
	runner := newRunner(
		nil,
		fakeS3ZipArchiveStore{
			openRange: func(_ context.Context, ref locator.Ref, offset int64, length int64) (io.ReadCloser, error) {
				rangeCalls++
				if ref.Key != "bundle.zip" {
					t.Fatalf("ref.Key = %q, want %q", ref.Key, "bundle.zip")
				}
				end := min(offset+length, int64(len(payload)))
				return io.NopCloser(bytes.NewReader(payload[offset:end])), nil
			},
		},
		nil,
		io.Discard,
		io.Discard,
	)

	warnings, err := runner.withZipReader(
		context.Background(),
		locator.Ref{Kind: locator.KindS3, Raw: "s3://bucket/bundle.zip", Bucket: "bucket", Key: "bundle.zip"},
		stream,
		archiveReaderInfo{Size: int64(len(payload)), SizeKnown: true},
		nil,
		func(zr *zip.Reader) (int, error) {
			if len(zr.File) != 1 || zr.File[0].Name != "file.txt" {
				t.Fatalf("zip files = %+v", zr.File)
			}
			return 0, nil
		},
	)
	if err != nil {
		t.Fatalf("withZipReader() error = %v", err)
	}
	if warnings != 0 {
		t.Fatalf("warnings = %d, want 0", warnings)
	}
	if stream.readCalls != 0 {
		t.Fatalf("stream readCalls = %d, want 0", stream.readCalls)
	}
	if stream.closeCalls != 1 {
		t.Fatalf("stream closeCalls = %d, want 1", stream.closeCalls)
	}
	if rangeCalls == 0 {
		t.Fatal("expected remote range reads")
	}
}

// TestWithZipReaderFallsBackToStagingWhenRemoteRangesFail verifies that range
// initialization failures still fall back to the existing temp-file path.
func TestWithZipReaderFallsBackToStagingWhenRemoteRangesFail(t *testing.T) {
	payload := zipArchiveBytes(t, map[string]string{
		"file.txt": "payload",
	})
	stream := &trackingReadCloser{Reader: bytes.NewReader(payload)}
	runner := newRunner(
		nil,
		nil,
		fakeHTTPZipArchiveStore{
			openRange: func(_ context.Context, ref locator.Ref, offset int64, length int64) (io.ReadCloser, error) {
				return nil, errors.New("range unsupported")
			},
		},
		io.Discard,
		io.Discard,
	)

	warnings, err := runner.withZipReader(
		context.Background(),
		locator.Ref{Kind: locator.KindHTTP, Raw: "https://example.test/bundle.zip", URL: "https://example.test/bundle.zip"},
		stream,
		archiveReaderInfo{Size: int64(len(payload)), SizeKnown: true},
		nil,
		func(zr *zip.Reader) (int, error) {
			if len(zr.File) != 1 || zr.File[0].Name != "file.txt" {
				t.Fatalf("zip files = %+v", zr.File)
			}
			return 0, nil
		},
	)
	if err != nil {
		t.Fatalf("withZipReader() error = %v", err)
	}
	if warnings != 0 {
		t.Fatalf("warnings = %d, want 0", warnings)
	}
	if stream.readCalls == 0 {
		t.Fatal("expected staging fallback to read the original stream")
	}
}
