package engine

import (
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/islishude/gotgz/internal/locator"
)

// TestWithZipReaderRespectsContextDuringTempCopy verifies staging copy aborts
// promptly when context cancellation happens mid-stream.
func TestWithZipReaderRespectsContextDuringTempCopy(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	src := newSlowChunkReader(5000, 1024, 2*time.Millisecond)
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

// slowChunkReader emits fixed-size chunks with delay to model a long stream.
type slowChunkReader struct {
	started   chan struct{}
	startOnce sync.Once
	remaining int
	delay     time.Duration
	chunk     []byte
}

// newSlowChunkReader creates a deterministic reader for cancellation tests.
func newSlowChunkReader(chunks, chunkSize int, delay time.Duration) *slowChunkReader {
	if chunkSize <= 0 {
		chunkSize = 1
	}
	return &slowChunkReader{
		started:   make(chan struct{}),
		remaining: chunks,
		delay:     delay,
		chunk:     bytes.Repeat([]byte{'x'}, chunkSize),
	}
}

// Read returns one delayed chunk until the configured chunk budget is exhausted.
func (r *slowChunkReader) Read(p []byte) (int, error) {
	if r.remaining <= 0 {
		return 0, io.EOF
	}
	r.startOnce.Do(func() {
		close(r.started)
	})
	time.Sleep(r.delay)
	r.remaining--
	return copy(p, r.chunk), nil
}

// waitForStart blocks until at least one read has been attempted.
func (r *slowChunkReader) waitForStart(t *testing.T) {
	t.Helper()
	select {
	case <-r.started:
	case <-time.After(time.Second):
		t.Fatal("reader did not start")
	}
}
