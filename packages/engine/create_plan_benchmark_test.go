package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/islishude/gotgz/packages/cli"
	"github.com/islishude/gotgz/packages/locator"
	localstore "github.com/islishude/gotgz/packages/storage/local"
)

type fullPlanBenchmarkStore struct {
	store *localstore.ArchiveStore
}

func (s fullPlanBenchmarkStore) OpenReader(ref locator.Ref) (io.ReadCloser, localstore.Metadata, error) {
	return s.store.OpenReader(ref)
}

func (s fullPlanBenchmarkStore) OpenWriter(ref locator.Ref) (io.WriteCloser, error) {
	return s.store.OpenWriter(ref)
}

func BenchmarkSpoolLocalCreateRecords(b *testing.B) {
	const entryCount = 10_000
	root := createPlanBenchmarkFixture(b, entryCount)
	spoolDir := b.TempDir()
	b.ReportAllocs()
	b.ReportMetric(entryCount, "entries/op")
	b.ResetTimer()

	for range b.N {
		path, _, count, err := spoolLocalCreateRecords(context.Background(), spoolDir, "source", root, nil, nil)
		if err != nil {
			b.Fatalf("spoolLocalCreateRecords() error = %v", err)
		}
		if count != entryCount+1 {
			b.Fatalf("record count = %d, want %d", count, entryCount+1)
		}
		if err := removePlanFile(path); err != nil {
			b.Fatalf("removePlanFile() error = %v", err)
		}
	}
}

func BenchmarkSpoolLocalCreateRecordsConcurrency(b *testing.B) {
	const entryCount = 10_000
	root := createPlanBenchmarkFixture(b, entryCount)
	for _, concurrency := range []int{1, 2, 4, 8, 16} {
		b.Run(fmt.Sprintf("workers-%d", concurrency), func(b *testing.B) {
			spoolDir := b.TempDir()
			limiter := newCreatePlanMetadataLimiter(concurrency)
			b.ReportAllocs()
			b.ReportMetric(entryCount, "entries/op")
			b.ResetTimer()
			for range b.N {
				path, _, count, err := spoolLocalCreateRecordsWithLimiter(context.Background(), spoolDir, "source", root, nil, nil, limiter)
				if err != nil {
					b.Fatalf("spoolLocalCreateRecordsWithLimiter() error = %v", err)
				}
				if count != entryCount+1 {
					b.Fatalf("record count = %d, want %d", count, entryCount+1)
				}
				if err := removePlanFile(path); err != nil {
					b.Fatalf("removePlanFile() error = %v", err)
				}
			}
		})
	}
}

func createPlanBenchmarkFixture(b *testing.B, entryCount int) string {
	b.Helper()
	root := b.TempDir()
	source := filepath.Join(root, "source")
	if err := os.Mkdir(source, 0o755); err != nil {
		b.Fatalf("Mkdir(source) error = %v", err)
	}
	for index := range entryCount {
		name := filepath.Join(source, fmt.Sprintf("file-%06d", index))
		if err := os.WriteFile(name, nil, 0o600); err != nil {
			b.Fatalf("WriteFile(%q) error = %v", name, err)
		}
	}
	return root
}

func BenchmarkCreatePlanningStrategies(b *testing.B) {
	const (
		entryCount  = 10_000
		payloadSize = 4 << 10
	)
	root := b.TempDir()
	source := filepath.Join(root, "source")
	if err := os.Mkdir(source, 0o755); err != nil {
		b.Fatalf("Mkdir(source) error = %v", err)
	}
	payload := make([]byte, payloadSize)
	state := uint64(0x9e3779b97f4a7c15)
	for index := range entryCount {
		for offset := range payload {
			state ^= state << 13
			state ^= state >> 7
			state ^= state << 17
			payload[offset] = byte(state)
		}
		name := filepath.Join(source, fmt.Sprintf("file-%06d", index))
		if err := os.WriteFile(name, payload, 0o600); err != nil {
			b.Fatalf("WriteFile(%q) error = %v", name, err)
		}
	}

	stores := []struct {
		name  string
		local localArchiveStore
	}{
		{name: "full-plan", local: fullPlanBenchmarkStore{store: &localstore.ArchiveStore{}}},
		{name: "streaming-plan", local: &localstore.ArchiveStore{}},
	}
	for _, store := range stores {
		b.Run(store.name, func(b *testing.B) {
			b.SetBytes(entryCount * payloadSize)
			runner := newRunner(store.local, nil, nil, io.Discard, io.Discard)
			b.ResetTimer()
			for index := range b.N {
				result := runner.Run(context.Background(), cli.Options{
					Mode:        cli.ModeCreate,
					Archive:     filepath.Join(root, fmt.Sprintf("%s-%d.tar.gz", store.name, index)),
					Chdir:       root,
					Members:     []string{"source"},
					Progress:    cli.ProgressNever,
					Compression: cli.CompressionGzip,
				})
				if result.ExitCode != ExitSuccess {
					b.Fatalf("Run() = %+v", result)
				}
			}
		})
	}
}

func BenchmarkCreateTimeToFirstPayload(b *testing.B) {
	const entryCount = 10_000
	root := createPlanBenchmarkFixture(b, entryCount)
	runner := newRunner(&localstore.ArchiveStore{}, nil, nil, io.Discard, io.Discard)
	opts := cli.Options{
		Archive: filepath.Join(root, "archive.tar"),
		Chdir:   root,
		Members: []string{"source"},
	}
	stop := errors.New("first payload observed")

	for _, strategy := range []createStrategy{createStrategyFullPlan, createStrategyStreamingPlan} {
		name := "full-plan"
		if strategy == createStrategyStreamingPlan {
			name = "streaming-plan"
		}
		b.Run(name, func(b *testing.B) {
			var firstPayloadTotal time.Duration
			for range b.N {
				start := time.Now()
				request, err := runner.prepareCreateRequest(context.Background(), opts, nil)
				if err != nil {
					b.Fatalf("prepareCreateRequest() error = %v", err)
				}
				var source createInputSource
				switch strategy {
				case createStrategyFullPlan:
					plan, err := runner.buildPreparedCreatePlan(context.Background(), request)
					if err != nil {
						b.Fatalf("buildPreparedCreatePlan() error = %v", err)
					}
					source = plannedCreateInputSource{plan: plan}
				case createStrategyStreamingPlan:
					streaming, err := newStreamingCreateInputSource(request, nil)
					if err != nil {
						b.Fatalf("newStreamingCreateInputSource() error = %v", err)
					}
					source = streaming
				}
				_, err = source.Visit(context.Background(), nil, func(local localCreateSource) (int, error) {
					return visitLocalCreateSource(context.Background(), local, func(_ localCreateRecord, info fs.FileInfo) (int, error) {
						if !info.Mode().IsRegular() {
							return 0, nil
						}
						firstPayloadTotal += time.Since(start)
						return 0, stop
					})
				})
				if !errors.Is(err, stop) {
					b.Fatalf("Visit() error = %v, want first-payload sentinel", err)
				}
				if err := source.Close(); err != nil {
					b.Fatalf("source.Close() error = %v", err)
				}
			}
			b.ReportMetric(float64(firstPayloadTotal.Nanoseconds())/float64(b.N), "first-payload-ns/op")
		})
	}
}
