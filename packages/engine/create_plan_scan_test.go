package engine

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"
)

type collectingCreatePlanSink struct {
	records []createPlanRecord
}

func (s *collectingCreatePlanSink) Append(record createPlanRecord) error {
	s.records = append(s.records, record)
	return nil
}

func createPlanScanFixture(t *testing.T, names ...string) (root string, source string, spoolInfo fs.FileInfo) {
	t.Helper()
	root = t.TempDir()
	source = filepath.Join(root, "source")
	if err := os.Mkdir(source, 0o755); err != nil {
		t.Fatalf("Mkdir(source) error = %v", err)
	}
	for _, name := range names {
		if err := os.WriteFile(filepath.Join(source, name), nil, 0o600); err != nil {
			t.Fatalf("WriteFile(%q) error = %v", name, err)
		}
	}
	spoolDir := t.TempDir()
	info, err := os.Stat(spoolDir)
	if err != nil {
		t.Fatalf("Stat(spoolDir) error = %v", err)
	}
	return root, source, info
}

func TestScanLocalCreateRecordsCommitsOutOfOrderMetadataInWalkOrder(t *testing.T) {
	root, _, spoolInfo := createPlanScanFixture(t, "a", "b", "c")
	aStarted := make(chan struct{})
	bFinished := make(chan struct{})
	releaseA := make(chan struct{})
	config := newCreatePlanScannerConfig(newCreatePlanMetadataLimiter(3))
	config.workerCount = 3
	config.windowSize = 3
	config.metadata.lstat = func(ctx context.Context, path string) (fs.FileInfo, error) {
		switch filepath.Base(path) {
		case "a":
			close(aStarted)
			select {
			case <-releaseA:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		case "b":
			close(bFinished)
		}
		return os.Lstat(path)
	}

	sink := &collectingCreatePlanSink{}
	done := make(chan error, 1)
	go func() {
		_, _, err := scanLocalCreateRecords(context.Background(), "source", root, nil, nil, spoolInfo, sink, config)
		done <- err
	}()

	select {
	case <-aStarted:
	case <-time.After(time.Second):
		t.Fatal("first metadata job did not start")
	}
	select {
	case <-bFinished:
	case <-time.After(time.Second):
		t.Fatal("later metadata job did not finish concurrently")
	}
	close(releaseA)
	if err := <-done; err != nil {
		t.Fatalf("scanLocalCreateRecords() error = %v", err)
	}

	var names []string
	for _, record := range sink.records {
		names = append(names, record.ArchiveName)
	}
	if want := []string{"source", "source/a", "source/b", "source/c"}; !reflect.DeepEqual(names, want) {
		t.Fatalf("record order = %v, want %v", names, want)
	}
}

func TestScanLocalCreateRecordsReturnsEarliestSequenceError(t *testing.T) {
	root, _, spoolInfo := createPlanScanFixture(t, "a", "b")
	earlier := errors.New("earlier metadata failure")
	later := errors.New("later metadata failure")
	bFailed := make(chan struct{})
	config := newCreatePlanScannerConfig(newCreatePlanMetadataLimiter(2))
	config.workerCount = 2
	config.windowSize = 3
	config.metadata.lstat = func(ctx context.Context, path string) (fs.FileInfo, error) {
		switch filepath.Base(path) {
		case "a":
			select {
			case <-bFailed:
				return nil, earlier
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		case "b":
			close(bFailed)
			return nil, later
		default:
			return os.Lstat(path)
		}
	}

	_, _, err := scanLocalCreateRecords(context.Background(), "source", root, nil, nil, spoolInfo, &collectingCreatePlanSink{}, config)
	if !errors.Is(err, earlier) {
		t.Fatalf("scanLocalCreateRecords() error = %v, want earliest error", err)
	}
}

func TestScanLocalCreateRecordsBoundsOutstandingWindow(t *testing.T) {
	names := make([]string, 20)
	for index := range names {
		names[index] = string(rune('a' + index))
	}
	root, _, spoolInfo := createPlanScanFixture(t, names...)
	releaseFirst := make(chan struct{})
	windowFilled := make(chan struct{})
	var mu sync.Mutex
	started := 0
	config := newCreatePlanScannerConfig(newCreatePlanMetadataLimiter(8))
	config.workerCount = 8
	config.windowSize = 4
	config.metadata.lstat = func(ctx context.Context, path string) (fs.FileInfo, error) {
		if filepath.Base(path) == "source" {
			return os.Lstat(path)
		}
		mu.Lock()
		started++
		if started == config.windowSize {
			close(windowFilled)
		}
		mu.Unlock()
		if filepath.Base(path) == "a" {
			select {
			case <-releaseFirst:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return os.Lstat(path)
	}

	done := make(chan error, 1)
	go func() {
		_, _, err := scanLocalCreateRecords(context.Background(), "source", root, nil, nil, spoolInfo, &collectingCreatePlanSink{}, config)
		done <- err
	}()
	select {
	case <-windowFilled:
	case <-time.After(time.Second):
		t.Fatal("metadata window did not fill")
	}
	mu.Lock()
	gotStarted := started
	mu.Unlock()
	if gotStarted != config.windowSize {
		t.Fatalf("metadata jobs started while first sequence was blocked = %d, want bounded window %d", gotStarted, config.windowSize)
	}
	close(releaseFirst)
	if err := <-done; err != nil {
		t.Fatalf("scanLocalCreateRecords() error = %v", err)
	}
}

func TestScanLocalCreateRecordsCancellationStopsWorkers(t *testing.T) {
	root, _, spoolInfo := createPlanScanFixture(t, "a", "b", "c")
	started := make(chan struct{})
	config := newCreatePlanScannerConfig(newCreatePlanMetadataLimiter(3))
	config.workerCount = 3
	config.windowSize = 3
	config.metadata.lstat = func(ctx context.Context, path string) (fs.FileInfo, error) {
		if filepath.Base(path) == "source" {
			return os.Lstat(path)
		}
		select {
		case started <- struct{}{}:
		default:
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, _, err := scanLocalCreateRecords(ctx, "source", root, nil, nil, spoolInfo, &collectingCreatePlanSink{}, config)
		done <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("metadata worker did not start")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("scanLocalCreateRecords() error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("scanLocalCreateRecords() did not stop after cancellation")
	}
}

func TestScanLocalCreateRecordsSkipsEphemeralArtifactIdentityAlias(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source")
	if err := os.Mkdir(source, 0o755); err != nil {
		t.Fatalf("Mkdir(source) error = %v", err)
	}
	artifact := filepath.Join(root, ".archive.tar.gotgz-actual")
	if err := os.WriteFile(artifact, []byte("archive bytes"), 0o600); err != nil {
		t.Fatalf("WriteFile(artifact) error = %v", err)
	}
	if err := os.Link(artifact, filepath.Join(source, "artifact-alias")); err != nil {
		t.Fatalf("Link(artifact alias) error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(source, ".archive.tar.gotgz-legitimate"), []byte("user data"), 0o600); err != nil {
		t.Fatalf("WriteFile(legitimate) error = %v", err)
	}
	policy := &createOutputPolicy{}
	if err := policy.registerEphemeralLocalPaths([]string{artifact}); err != nil {
		t.Fatalf("registerEphemeralLocalPaths() error = %v", err)
	}
	spoolDir := t.TempDir()
	spoolInfo, err := os.Stat(spoolDir)
	if err != nil {
		t.Fatalf("Stat(spoolDir) error = %v", err)
	}
	sink := &collectingCreatePlanSink{}
	_, _, err = scanLocalCreateRecords(context.Background(), "source", root, nil, policy, spoolInfo, sink, newCreatePlanScannerConfig(nil))
	if err != nil {
		t.Fatalf("scanLocalCreateRecords() error = %v", err)
	}
	var names []string
	for _, record := range sink.records {
		names = append(names, record.ArchiveName)
	}
	want := []string{"source", "source/.archive.tar.gotgz-legitimate"}
	if !reflect.DeepEqual(names, want) {
		t.Fatalf("records = %v, want exact artifact identity skipped and legitimate name retained: %v", names, want)
	}
}
