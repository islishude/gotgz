package engine

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

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
