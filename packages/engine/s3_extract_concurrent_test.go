package engine

import (
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/islishude/gotgz/packages/archivepath"
	"github.com/islishude/gotgz/packages/archiveprogress"
	"github.com/islishude/gotgz/packages/cli"
	"github.com/islishude/gotgz/packages/locator"
)

func TestWithConcurrentS3ZipReaderStagesRemoteArchive(t *testing.T) {
	payload := orderedZipArchiveBytes(t, []zipTestEntry{{name: "file.txt", body: "payload"}})
	stream := &trackingReadCloser{Reader: bytes.NewReader(payload)}
	runner := newRunner(
		nil,
		fakeS3ZipArchiveStore{
			openRange: func(_ context.Context, _ locator.Ref, _, _ int64) (io.ReadCloser, error) {
				t.Fatal("remote ranges should not be used for concurrent S3 zip extract")
				return nil, nil
			},
		},
		nil,
		io.Discard,
		io.Discard,
		withS3ExtractConfig(s3ExtractConfig{
			workers:      2,
			stagingBytes: 1024,
			stagingDir:   t.TempDir(),
		}),
	)

	warnings, err := runner.withConcurrentS3ZipReader(
		context.Background(),
		locator.Ref{Kind: locator.KindS3, Raw: "s3://bucket/archive.zip", Bucket: "bucket", Key: "archive.zip"},
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
		t.Fatalf("withConcurrentS3ZipReader() error = %v", err)
	}
	if warnings != 0 {
		t.Fatalf("warnings = %d, want 0", warnings)
	}
	if stream.readCalls == 0 {
		t.Fatal("expected staged zip reader to consume the original stream")
	}
}

func TestExtractZipEntriesToS3ConcurrentRunsUploadsInParallel(t *testing.T) {
	payload := orderedZipArchiveBytes(t, []zipTestEntry{
		{name: "a.txt", body: "alpha"},
		{name: "b.txt", body: "bravo"},
	})
	zr, err := zip.NewReader(bytes.NewReader(payload), int64(len(payload)))
	if err != nil {
		t.Fatalf("zip.NewReader() error = %v", err)
	}

	var (
		mu      sync.Mutex
		current int
		maxSeen int
		bodies  = map[string]string{}
	)
	twoStarted := make(chan struct{})
	release := make(chan struct{})

	var stdout bytes.Buffer
	runner := newRunner(
		nil,
		fakeS3ArchiveStore{
			uploadStream: func(_ context.Context, ref locator.Ref, body io.Reader, _ map[string]string) error {
				mu.Lock()
				current++
				if current > maxSeen {
					maxSeen = current
				}
				if current == 2 {
					select {
					case <-twoStarted:
					default:
						close(twoStarted)
					}
				}
				mu.Unlock()

				<-release
				payload, err := io.ReadAll(body)
				if err != nil {
					return err
				}

				mu.Lock()
				bodies[ref.Key] = string(payload)
				current--
				mu.Unlock()
				return nil
			},
		},
		nil,
		&stdout,
		io.Discard,
		withS3ExtractConfig(s3ExtractConfig{
			workers:      2,
			stagingBytes: 1024,
			stagingDir:   t.TempDir(),
		}),
	)

	reporter := archiveprogress.NewReporter(io.Discard, cli.ProgressNever, 0, false, time.Now(), false)
	errCh := make(chan error, 1)
	go func() {
		_, err := runner.extractZipEntriesToS3Concurrent(
			context.Background(),
			zr,
			cli.Options{Verbose: true},
			reporter,
			locator.Ref{Kind: locator.KindS3, Bucket: "bucket", Key: "prefix"},
			archivepath.NewCompiledPathMatcher(nil),
		)
		errCh <- err
	}()

	select {
	case <-twoStarted:
	case <-time.After(time.Second):
		t.Fatal("zip uploads did not overlap")
	}
	close(release)

	if err := <-errCh; err != nil {
		t.Fatalf("extractZipEntriesToS3Concurrent() error = %v", err)
	}

	mu.Lock()
	gotMax := maxSeen
	gotBodies := map[string]string{
		"prefix/a.txt": bodies["prefix/a.txt"],
		"prefix/b.txt": bodies["prefix/b.txt"],
	}
	mu.Unlock()

	if gotMax < 2 {
		t.Fatalf("max concurrent uploads = %d, want at least 2", gotMax)
	}
	if gotBodies["prefix/a.txt"] != "alpha" || gotBodies["prefix/b.txt"] != "bravo" {
		t.Fatalf("uploaded bodies = %#v", gotBodies)
	}
	if stdout.String() != "a.txt\nb.txt\n" {
		t.Fatalf("stdout = %q, want archive-order verbose output", stdout.String())
	}
}

func TestExtractTarEntriesToS3ConcurrentStagesSmallFiles(t *testing.T) {
	stagingDir := t.TempDir()
	tr := newTarReaderFromEntries(t, []tarEntry{
		{hdr: newTarHeader("a.txt", 5, 0o644), body: "alpha"},
		{hdr: newTarHeader("b.txt", 5, 0o644), body: "bravo"},
	})

	var (
		mu      sync.Mutex
		current int
		maxSeen int
		bodies  = map[string]string{}
	)
	twoStarted := make(chan struct{})
	release := make(chan struct{})

	runner := newRunner(
		nil,
		fakeS3ArchiveStore{
			uploadStream: func(_ context.Context, ref locator.Ref, body io.Reader, _ map[string]string) error {
				if _, ok := body.(*os.File); !ok {
					t.Fatalf("body type = %T, want *os.File for staged upload", body)
				}

				mu.Lock()
				current++
				if current > maxSeen {
					maxSeen = current
				}
				if current == 2 {
					select {
					case <-twoStarted:
					default:
						close(twoStarted)
					}
				}
				mu.Unlock()

				<-release
				payload, err := io.ReadAll(body)
				if err != nil {
					return err
				}

				mu.Lock()
				bodies[ref.Key] = string(payload)
				current--
				mu.Unlock()
				return nil
			},
		},
		nil,
		io.Discard,
		io.Discard,
		withS3ExtractConfig(s3ExtractConfig{
			workers:      2,
			stagingBytes: 16,
			stagingDir:   stagingDir,
		}),
	)

	errCh := make(chan error, 1)
	go func() {
		_, err := runner.extractTarEntriesToS3Concurrent(context.Background(), locator.Ref{Kind: locator.KindS3, Bucket: "bucket", Key: "prefix"}, func(pipeline *s3ExtractPipeline, budget *s3ExtractStagingBudget) (int, error) {
			warnings := 0
			for {
				hdr, err := tr.Next()
				if errors.Is(err, io.EOF) {
					return warnings, nil
				}
				if err != nil {
					return warnings, err
				}
				w, err := runner.extractToS3Concurrent(context.Background(), locator.Ref{Kind: locator.KindS3, Bucket: "bucket", Key: "prefix"}, hdr, tr, nil, pipeline, budget)
				warnings += w
				if err != nil {
					return warnings, err
				}
			}
		})
		errCh <- err
	}()

	select {
	case <-twoStarted:
	case <-time.After(time.Second):
		t.Fatal("tar uploads did not overlap")
	}
	close(release)

	if err := <-errCh; err != nil {
		t.Fatalf("extractTarEntriesToS3Concurrent() error = %v", err)
	}

	mu.Lock()
	gotMax := maxSeen
	gotBodies := map[string]string{
		"prefix/a.txt": bodies["prefix/a.txt"],
		"prefix/b.txt": bodies["prefix/b.txt"],
	}
	mu.Unlock()

	if gotMax < 2 {
		t.Fatalf("max concurrent uploads = %d, want at least 2", gotMax)
	}
	if gotBodies["prefix/a.txt"] != "alpha" || gotBodies["prefix/b.txt"] != "bravo" {
		t.Fatalf("uploaded bodies = %#v", gotBodies)
	}
	entries, err := os.ReadDir(stagingDir)
	if err != nil {
		t.Fatalf("os.ReadDir(%q) error = %v", stagingDir, err)
	}
	if len(entries) != 0 {
		t.Fatalf("staging dir entries = %d, want 0", len(entries))
	}
}

func TestExtractToS3ConcurrentFallsBackToInlineUploadForLargeTarEntry(t *testing.T) {
	stagingDir := t.TempDir()
	tr := newTarReaderFromEntries(t, []tarEntry{{hdr: newTarHeader("large.txt", 8, 0o644), body: "payload!"}})
	hdr, err := tr.Next()
	if err != nil {
		t.Fatalf("tr.Next() error = %v", err)
	}

	var sawInline bool
	runner := newRunner(
		nil,
		fakeS3ArchiveStore{
			uploadStream: func(_ context.Context, ref locator.Ref, body io.Reader, _ map[string]string) error {
				if _, ok := body.(*os.File); ok {
					t.Fatalf("body type = %T, want inline stream", body)
				}
				sawInline = true
				payload, err := io.ReadAll(body)
				if err != nil {
					return err
				}
				if string(payload) != "payload!" {
					t.Fatalf("payload = %q, want %q", string(payload), "payload!")
				}
				if ref.Key != "prefix/large.txt" {
					t.Fatalf("ref.Key = %q, want %q", ref.Key, "prefix/large.txt")
				}
				return nil
			},
		},
		nil,
		io.Discard,
		io.Discard,
		withS3ExtractConfig(s3ExtractConfig{
			workers:      2,
			stagingBytes: 4,
			stagingDir:   stagingDir,
		}),
	)

	pipeline := newS3ExtractPipeline(context.Background(), runner.s3Extract.workers)
	budget := newS3ExtractStagingBudget(runner.s3Extract.stagingBytes)
	_, err = runner.extractToS3Concurrent(context.Background(), locator.Ref{Kind: locator.KindS3, Bucket: "bucket", Key: "prefix"}, hdr, tr, nil, pipeline, budget)
	if err != nil {
		t.Fatalf("extractToS3Concurrent() error = %v", err)
	}
	if err := pipeline.Wait(); err != nil {
		t.Fatalf("pipeline.Wait() error = %v", err)
	}
	if !sawInline {
		t.Fatal("expected inline upload for oversized tar entry")
	}
	entries, err := os.ReadDir(stagingDir)
	if err != nil {
		t.Fatalf("os.ReadDir(%q) error = %v", stagingDir, err)
	}
	if len(entries) != 0 {
		t.Fatalf("staging dir entries = %d, want 0", len(entries))
	}
}

func TestExtractToS3ConcurrentReleasesBudgetAfterUploadError(t *testing.T) {
	stagingDir := t.TempDir()
	tr := newTarReaderFromEntries(t, []tarEntry{{hdr: newTarHeader("fail.txt", 4, 0o644), body: "boom"}})
	hdr, err := tr.Next()
	if err != nil {
		t.Fatalf("tr.Next() error = %v", err)
	}

	wantErr := errors.New("upload failed")
	runner := newRunner(
		nil,
		fakeS3ArchiveStore{
			uploadStream: func(_ context.Context, _ locator.Ref, body io.Reader, _ map[string]string) error {
				if _, ok := body.(*os.File); !ok {
					t.Fatalf("body type = %T, want *os.File for staged upload", body)
				}
				_, _ = io.ReadAll(body)
				return wantErr
			},
		},
		nil,
		io.Discard,
		io.Discard,
		withS3ExtractConfig(s3ExtractConfig{
			workers:      2,
			stagingBytes: 4,
			stagingDir:   stagingDir,
		}),
	)

	pipeline := newS3ExtractPipeline(context.Background(), runner.s3Extract.workers)
	budget := newS3ExtractStagingBudget(runner.s3Extract.stagingBytes)
	_, err = runner.extractToS3Concurrent(context.Background(), locator.Ref{Kind: locator.KindS3, Bucket: "bucket", Key: "prefix"}, hdr, tr, nil, pipeline, budget)
	if err != nil {
		t.Fatalf("extractToS3Concurrent() error = %v", err)
	}
	if err := pipeline.Wait(); !errors.Is(err, wantErr) {
		t.Fatalf("pipeline.Wait() error = %v, want %v", err, wantErr)
	}

	acquireCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := budget.Acquire(acquireCtx, 4); err != nil {
		t.Fatalf("budget.Acquire() error = %v", err)
	}
	budget.Release(4)

	entries, err := os.ReadDir(stagingDir)
	if err != nil {
		t.Fatalf("os.ReadDir(%q) error = %v", stagingDir, err)
	}
	if len(entries) != 0 {
		t.Fatalf("staging dir entries = %d, want 0", len(entries))
	}
}

type zipTestEntry struct {
	name string
	body string
}

func orderedZipArchiveBytes(t *testing.T, entries []zipTestEntry) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	for _, entry := range entries {
		w, err := zw.Create(entry.name)
		if err != nil {
			t.Fatalf("zw.Create(%q) error = %v", entry.name, err)
		}
		if _, err := io.WriteString(w, entry.body); err != nil {
			t.Fatalf("io.WriteString(%q) error = %v", entry.name, err)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("zw.Close() error = %v", err)
	}
	return buf.Bytes()
}
