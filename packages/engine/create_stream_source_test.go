package engine

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/islishude/gotgz/packages/cli"
	localstore "github.com/islishude/gotgz/packages/storage/local"
)

type recordingCreateTotalReporter struct {
	once  sync.Once
	total chan int64
}

func (r *recordingCreateTotalReporter) SetTotal(total int64, known bool) {
	if !known {
		return
	}
	r.once.Do(func() { r.total <- total })
}

func newStreamingSourceFixture(t *testing.T, files map[string]string, members ...string) (*streamingCreateInputSource, string, *recordingCreateTotalReporter) {
	t.Helper()
	root := t.TempDir()
	for name, body := range files {
		path := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("MkdirAll(%q) error = %v", path, err)
		}
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatalf("WriteFile(%q) error = %v", path, err)
		}
	}
	if len(members) == 0 {
		members = []string{"source"}
	}
	archivePath := filepath.Join(root, "archive.tar")
	runner := newRunner(&localstore.ArchiveStore{}, nil, nil, io.Discard, io.Discard)
	request, err := runner.prepareCreateRequest(context.Background(), cli.Options{
		Archive: archivePath,
		Chdir:   root,
		Members: members,
	}, nil)
	if err != nil {
		t.Fatalf("prepareCreateRequest() error = %v", err)
	}
	reporter := &recordingCreateTotalReporter{total: make(chan int64, 1)}
	source, err := newStreamingCreateInputSource(request, reporter)
	if err != nil {
		t.Fatalf("newStreamingCreateInputSource() error = %v", err)
	}
	t.Cleanup(func() {
		if err := source.Close(); err != nil {
			t.Errorf("source.Close() error = %v", err)
		}
	})
	return source, root, reporter
}

func TestStreamingCreateSourceWriterConsumesBeforeScannerCompletes(t *testing.T) {
	source, _, _ := newStreamingSourceFixture(t, map[string]string{
		"source/a": "a",
		"source/b": "b",
	})
	bStarted := make(chan struct{})
	releaseB := make(chan struct{})
	aVisited := make(chan struct{})
	defaults := defaultCreatePlanMetadataOps()
	source.scannerConfig.metadata.lstat = func(ctx context.Context, path string) (fs.FileInfo, error) {
		if filepath.Base(path) == "b" {
			close(bStarted)
			select {
			case <-releaseB:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return defaults.lstat(ctx, path)
	}

	done := make(chan error, 1)
	go func() {
		_, err := source.Visit(context.Background(), nil, func(local localCreateSource) (int, error) {
			return visitLocalCreateSource(context.Background(), local, func(record localCreateRecord, _ fs.FileInfo) (int, error) {
				if record.archiveName == "source/a" {
					close(aVisited)
				}
				return 0, nil
			})
		})
		done <- err
	}()

	select {
	case <-bStarted:
	case <-time.After(time.Second):
		t.Fatal("scanner did not reach blocked later entry")
	}
	select {
	case <-aVisited:
	case <-time.After(time.Second):
		t.Fatal("writer did not consume an earlier entry while scanner was blocked")
	}
	close(releaseB)
	if err := <-done; err != nil {
		t.Fatalf("Visit() error = %v", err)
	}
}

func TestStreamingCreateSourceScannerCompletesWhileWriterIsBlocked(t *testing.T) {
	source, _, reporter := newStreamingSourceFixture(t, map[string]string{
		"source/a": "one",
		"source/b": "two",
		"source/c": "three",
	})
	writerStarted := make(chan struct{})
	releaseWriter := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, err := source.Visit(context.Background(), nil, func(local localCreateSource) (int, error) {
			first := true
			return visitLocalCreateSource(context.Background(), local, func(localCreateRecord, fs.FileInfo) (int, error) {
				if first {
					first = false
					close(writerStarted)
					<-releaseWriter
				}
				return 0, nil
			})
		})
		done <- err
	}()

	select {
	case <-writerStarted:
	case <-time.After(time.Second):
		t.Fatal("writer did not start")
	}
	select {
	case total := <-reporter.total:
		if total != int64(len("one")+len("two")+len("three")) {
			t.Fatalf("exact total = %d, want 11", total)
		}
	case <-time.After(time.Second):
		t.Fatal("scanner did not finish and publish the exact total while writer was blocked")
	}
	close(releaseWriter)
	if err := <-done; err != nil {
		t.Fatalf("Visit() error = %v", err)
	}
}

func TestStreamingCreateSourcePreservesTopLevelMemberOrder(t *testing.T) {
	source, _, _ := newStreamingSourceFixture(t, map[string]string{
		"first/a":  "a",
		"second/b": "b",
	}, "first", "second")
	var seen []string
	_, err := source.Visit(context.Background(), nil, func(local localCreateSource) (int, error) {
		return visitLocalCreateSource(context.Background(), local, func(record localCreateRecord, _ fs.FileInfo) (int, error) {
			seen = append(seen, record.archiveName)
			return 0, nil
		})
	})
	if err != nil {
		t.Fatalf("Visit() error = %v", err)
	}
	want := []string{"first", "first/a", "second", "second/b"}
	if !reflect.DeepEqual(seen, want) {
		t.Fatalf("member order = %v, want %v", seen, want)
	}
}

func TestStreamingCreateSourceWriterFailureCancelsScanner(t *testing.T) {
	source, _, _ := newStreamingSourceFixture(t, map[string]string{
		"source/a": "a",
		"source/b": "b",
	})
	metadataBlocked := make(chan struct{})
	defaults := defaultCreatePlanMetadataOps()
	source.scannerConfig.metadata.lstat = func(ctx context.Context, path string) (fs.FileInfo, error) {
		if filepath.Base(path) == "b" {
			close(metadataBlocked)
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return defaults.lstat(ctx, path)
	}
	writerErr := errors.New("writer failed")
	done := make(chan error, 1)
	go func() {
		_, err := source.Visit(context.Background(), nil, func(local localCreateSource) (int, error) {
			return visitLocalCreateSource(context.Background(), local, func(localCreateRecord, fs.FileInfo) (int, error) {
				<-metadataBlocked
				return 0, writerErr
			})
		})
		done <- err
	}()
	select {
	case <-metadataBlocked:
	case <-time.After(time.Second):
		t.Fatal("scanner metadata job did not block")
	}
	select {
	case err := <-done:
		if !errors.Is(err, writerErr) {
			t.Fatalf("Visit() error = %v, want writer error", err)
		}
	case <-time.After(time.Second):
		t.Fatal("writer failure did not cancel and join scanner workers")
	}
}

func TestStreamingCreateSourceExcludesSpoolDirectoryInsideInput(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "payload"), []byte("payload"), 0o600); err != nil {
		t.Fatalf("WriteFile(payload) error = %v", err)
	}
	t.Setenv("TMPDIR", root)
	runner := newRunner(&localstore.ArchiveStore{}, nil, nil, io.Discard, io.Discard)
	request, err := runner.prepareCreateRequest(context.Background(), cli.Options{
		Archive: filepath.Join(root, "archive.tar"),
		Chdir:   root,
		Members: []string{"."},
	}, nil)
	if err != nil {
		t.Fatalf("prepareCreateRequest() error = %v", err)
	}
	source, err := newStreamingCreateInputSource(request, nil)
	if err != nil {
		t.Fatalf("newStreamingCreateInputSource() error = %v", err)
	}
	defer func() { _ = source.Close() }()
	var seen []string
	_, err = source.Visit(context.Background(), nil, func(local localCreateSource) (int, error) {
		return visitLocalCreateSource(context.Background(), local, func(record localCreateRecord, _ fs.FileInfo) (int, error) {
			seen = append(seen, record.archiveName)
			return 0, nil
		})
	})
	if err != nil {
		t.Fatalf("Visit() error = %v", err)
	}
	if got := strings.Join(seen, ","); got != ".,payload" {
		t.Fatalf("records = %q, want root and payload without private spool", got)
	}
}
