package engine

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/islishude/gotgz/packages/cli"
	"github.com/islishude/gotgz/packages/locator"
)

func TestSplitExtractPlannerFinalizeLocal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		manifest     []splitExtractManifestEntry
		wantParallel bool
		wantReason   splitExtractSerialReason
	}{
		{
			name: "duplicate file across volumes falls back",
			manifest: []splitExtractManifestEntry{
				{volumeIndex: 0, outputPath: "dir/file.txt"},
				{volumeIndex: 1, outputPath: "dir/file.txt"},
			},
			wantReason: splitExtractSerialReasonLocalDuplicate,
		},
		{
			name: "directory duplicate stays parallel",
			manifest: []splitExtractManifestEntry{
				{volumeIndex: 0, outputPath: "dir", isDir: true},
				{volumeIndex: 1, outputPath: "dir", isDir: true},
			},
			wantParallel: true,
		},
		{
			name: "non directory ancestor falls back",
			manifest: []splitExtractManifestEntry{
				{volumeIndex: 0, outputPath: "dir"},
				{volumeIndex: 1, outputPath: "dir/file.txt"},
			},
			wantReason: splitExtractSerialReasonLocalAncestor,
		},
		{
			name: "cross volume hardlink dependency falls back",
			manifest: []splitExtractManifestEntry{
				{volumeIndex: 0, outputPath: "alias.txt", hardlinkTarget: "target.txt"},
				{volumeIndex: 1, outputPath: "target.txt"},
			},
			wantReason: splitExtractSerialReasonLocalHardlink,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			planner := &splitExtractPlanner{
				target:   locator.Ref{Kind: locator.KindLocal, Path: "."},
				manifest: tt.manifest,
			}
			plan := planner.finalize()
			if plan.parallel != tt.wantParallel {
				t.Fatalf("parallel = %v, want %v", plan.parallel, tt.wantParallel)
			}
			if plan.serialReason != tt.wantReason {
				t.Fatalf("serialReason = %q, want %q", plan.serialReason, tt.wantReason)
			}
		})
	}
}

func TestSplitExtractPlannerFinalizeS3(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		manifest     []splitExtractManifestEntry
		wantParallel bool
		wantReason   splitExtractSerialReason
	}{
		{
			name: "duplicate object key falls back",
			manifest: []splitExtractManifestEntry{
				{volumeIndex: 0, outputPath: "prefix/file.txt"},
				{volumeIndex: 1, outputPath: "prefix/file.txt"},
			},
			wantReason: splitExtractSerialReasonS3Duplicate,
		},
		{
			name: "unique object keys stay parallel",
			manifest: []splitExtractManifestEntry{
				{volumeIndex: 0, outputPath: "prefix/one.txt"},
				{volumeIndex: 1, outputPath: "prefix/two.txt"},
			},
			wantParallel: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			planner := &splitExtractPlanner{
				target:   locator.Ref{Kind: locator.KindS3, Bucket: "bucket", Key: "prefix"},
				manifest: tt.manifest,
			}
			plan := planner.finalize()
			if plan.parallel != tt.wantParallel {
				t.Fatalf("parallel = %v, want %v", plan.parallel, tt.wantParallel)
			}
			if plan.serialReason != tt.wantReason {
				t.Fatalf("serialReason = %q, want %q", plan.serialReason, tt.wantReason)
			}
		})
	}
}

func TestPlanSplitZipExtractCountsPayloadBytes(t *testing.T) {
	root := t.TempDir()
	firstPart := filepath.Join(root, "bundle.part0001.zip")
	secondPart := filepath.Join(root, "bundle.part0002.zip")

	writeZipTestVolume(t, firstPart, []zipTestEntry{
		{name: "root/keep.txt", body: "one", mode: 0o644},
		{name: "root/skip.txt", body: "skip", mode: 0o644},
	})
	writeZipTestVolume(t, secondPart, []zipTestEntry{
		{name: "root/dir/", mode: os.ModeDir | 0o755},
		{name: "root/dir/nested.txt", body: "two!", mode: 0o644},
	})

	r := newLocalSplitExtractTestRunner()
	volumes := []archiveVolume{
		localArchiveVolumeFromPath(t, firstPart),
		localArchiveVolumeFromPath(t, secondPart),
	}
	firstReader, firstInfo, err := r.openArchiveReader(context.Background(), volumes[0].ref)
	if err != nil {
		t.Fatalf("openArchiveReader(%s): %v", firstPart, err)
	}
	defer firstReader.Close() //nolint:errcheck

	target, err := locator.ParseExtractTarget("", "", nil)
	if err != nil {
		t.Fatalf("ParseExtractTarget() error = %v", err)
	}

	plan, err := r.planSplitZipExtract(context.Background(), cli.Options{
		Mode:            cli.ModeExtract,
		Archive:         firstPart,
		Members:         []string{"root/keep.txt", "root/dir/nested.txt"},
		StripComponents: 1,
	}, volumes, firstReader, firstInfo, target)
	if err != nil {
		t.Fatalf("planSplitZipExtract() error = %v", err)
	}

	if !plan.parallel {
		t.Fatalf("parallel = false, want true")
	}
	if plan.zipPayloadBytes != int64(len("one")+len("two!")) {
		t.Fatalf("zipPayloadBytes = %d, want %d", plan.zipPayloadBytes, len("one")+len("two!"))
	}
}

func TestExecuteSplitExtractVolumesStartsWorkersConcurrently(t *testing.T) {
	t.Parallel()

	r := &Runner{}
	volumes := []archiveVolume{
		{ref: locator.Ref{Raw: "vol1"}},
		{ref: locator.Ref{Raw: "vol2"}},
	}
	started := make(chan int, len(volumes))
	release := make(chan struct{})
	done := make(chan struct {
		warnings int
		err      error
	}, 1)

	go func() {
		warnings, err := r.executeSplitExtractVolumes(context.Background(), volumes, func(ctx context.Context, index int, _ archiveVolume) (int, error) {
			started <- index
			<-release
			return index + 1, nil
		})
		done <- struct {
			warnings int
			err      error
		}{warnings: warnings, err: err}
	}()

	seen := make(map[int]struct{}, len(volumes))
	for len(seen) < len(volumes) {
		select {
		case index := <-started:
			seen[index] = struct{}{}
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for concurrent workers to start")
		}
	}
	close(release)

	result := <-done
	if result.err != nil {
		t.Fatalf("executeSplitExtractVolumes() error = %v", result.err)
	}
	if result.warnings != 3 {
		t.Fatalf("warnings = %d, want 3", result.warnings)
	}
}

func TestExecuteSplitExtractVolumesCancelsOnFirstError(t *testing.T) {
	t.Parallel()

	r := &Runner{}
	volumes := []archiveVolume{
		{ref: locator.Ref{Raw: "vol1"}},
		{ref: locator.Ref{Raw: "vol2"}},
	}
	started := make(chan int, len(volumes))
	release := make(chan struct{})
	errBoom := errors.New("boom")
	var canceled atomic.Bool
	done := make(chan error, 1)

	go func() {
		_, err := r.executeSplitExtractVolumes(context.Background(), volumes, func(ctx context.Context, index int, _ archiveVolume) (int, error) {
			started <- index
			<-release
			if index == 0 {
				return 0, errBoom
			}
			<-ctx.Done()
			canceled.Store(true)
			return 0, nil
		})
		done <- err
	}()

	seen := make(map[int]struct{}, len(volumes))
	for len(seen) < len(volumes) {
		select {
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for workers to start")
		case index := <-started:
			seen[index] = struct{}{}
		}
	}
	close(release)

	if err := <-done; !errors.Is(err, errBoom) {
		t.Fatalf("executeSplitExtractVolumes() error = %v, want %v", err, errBoom)
	}
	if !canceled.Load() {
		t.Fatalf("expected second worker to observe cancellation")
	}
}

func TestWarnfSerializesConcurrentOutput(t *testing.T) {
	t.Parallel()

	writer := newBlockingConcurrentWriter()
	r := &Runner{stderr: writer}

	var workers atomic.Int32
	done := make(chan struct{}, 8)
	for range 8 {
		workers.Add(1)
		go func() {
			defer func() {
				workers.Add(-1)
				done <- struct{}{}
			}()
			r.warnf(nil, "concurrent warning")
		}()
	}
	writer.waitForWrite(t)
	writer.unblock()
	for range 8 {
		<-done
	}

	if writer.maxConcurrent.Load() != 1 {
		t.Fatalf("max concurrent writes = %d, want 1", writer.maxConcurrent.Load())
	}
}
