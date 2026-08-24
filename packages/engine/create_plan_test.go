package engine

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/islishude/gotgz/packages/cli"
	"github.com/islishude/gotgz/packages/locator"
	"github.com/islishude/gotgz/packages/storage/s3"
)

func cleanupCreatePlan(t *testing.T, plan *createPlan) {
	t.Helper()
	t.Cleanup(func() {
		if err := plan.Close(); err != nil {
			t.Errorf("createPlan.Close() error = %v", err)
		}
	})
}

func TestBuildCreatePlanPreservesInputOrderAcrossConcurrentCompletion(t *testing.T) {
	fastDone := make(chan struct{})
	runner := newRunner(nil, fakeS3ArchiveStore{stat: func(_ context.Context, ref locator.Ref) (s3.Metadata, error) {
		if ref.Key == "slow" {
			<-fastDone
			return s3.Metadata{Size: 1}, nil
		}
		close(fastDone)
		return s3.Metadata{Size: 1}, nil
	}}, nil, io.Discard, io.Discard)

	plan, err := runner.buildCreatePlan(context.Background(), cli.Options{Members: []string{"s3://bucket/slow", "s3://bucket/fast"}}, nil)
	if err != nil {
		t.Fatalf("buildCreatePlan() error = %v", err)
	}
	cleanupCreatePlan(t, plan)
	if plan.spoolDir != "" {
		t.Fatalf("S3-only plan spoolDir = %q, want empty", plan.spoolDir)
	}
	if len(plan.members) != 2 || plan.members[0].ref.Key != "slow" || plan.members[1].ref.Key != "fast" {
		t.Fatalf("plan order = %+v, want slow then fast", plan.members)
	}
}

func TestBuildCreatePlanUsesPrivateDiskSpoolAndCleansIt(t *testing.T) {
	root := t.TempDir()
	t.Setenv("TMPDIR", root)
	if err := os.WriteFile(filepath.Join(root, "file.txt"), []byte("payload"), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	plan, err := (&Runner{}).buildCreatePlan(context.Background(), cli.Options{Members: []string{"."}, Chdir: root}, nil)
	if err != nil {
		t.Fatalf("buildCreatePlan() error = %v", err)
	}
	cleanupCreatePlan(t, plan)
	if plan.spoolDir == "" {
		t.Fatal("local plan spoolDir is empty")
	}
	dirInfo, err := os.Stat(plan.spoolDir)
	if err != nil {
		t.Fatalf("Stat(spoolDir) error = %v", err)
	}
	if got := dirInfo.Mode().Perm(); got != 0o700 {
		t.Fatalf("spool directory mode = %o, want 700", got)
	}
	if len(plan.members) != 1 || plan.members[0].localRecordCount != 2 {
		t.Fatalf("planned member = %+v, want root and file only", plan.members)
	}
	planPath := plan.members[0].localPlanPath
	fileInfo, err := os.Stat(planPath)
	if err != nil {
		t.Fatalf("Stat(plan file) error = %v", err)
	}
	if got := fileInfo.Mode().Perm(); got != 0o600 {
		t.Fatalf("plan file mode = %o, want 600", got)
	}
	var seen []string
	err = plannedLocalCreateSource{planPath: planPath}.Visit(context.Background(), func(record localCreateRecord, _ fs.FileInfo) error {
		seen = append(seen, record.archiveName)
		return nil
	})
	if err != nil {
		t.Fatalf("planned source Visit() error = %v", err)
	}
	if got := strings.Join(seen, ","); got != ".,file.txt" {
		t.Fatalf("planned records = %q, want root and file without spool", got)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := (plannedLocalCreateSource{planPath: planPath}).Visit(canceled, func(localCreateRecord, fs.FileInfo) error { return nil }); !errors.Is(err, context.Canceled) {
		t.Fatalf("planned source canceled Visit() error = %v, want context.Canceled", err)
	}
	spoolDir := plan.spoolDir
	if err := plan.Close(); err != nil {
		t.Fatalf("createPlan.Close() error = %v", err)
	}
	if _, err := os.Stat(spoolDir); !os.IsNotExist(err) {
		t.Fatalf("spool directory still exists: %v", err)
	}
}

func TestBuildCreatePlanFailsBeforeDestinationWhenTempStorageUnavailable(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "file.txt"), []byte("payload"), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	missingTempRoot := filepath.Join(root, "missing-temp-root")
	t.Setenv("TMPDIR", missingTempRoot)
	t.Setenv("TMP", missingTempRoot)
	t.Setenv("TEMP", missingTempRoot)
	if _, err := (&Runner{}).buildCreatePlan(context.Background(), cli.Options{Members: []string{"file.txt"}, Chdir: root}, nil); err == nil || !strings.Contains(err.Error(), "create plan spool directory") {
		t.Fatalf("buildCreatePlan() error = %v, want temp storage failure", err)
	}
}

func TestReplayLocalCreateRecordsRejectsCorruptSpool(t *testing.T) {
	path := filepath.Join(t.TempDir(), "corrupt.gob")
	if err := os.WriteFile(path, []byte("not a gob stream"), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	if err := replayLocalCreateRecords(context.Background(), path, func(localCreateRecord, fs.FileInfo) error { return nil }); err == nil || !strings.Contains(err.Error(), "decode local plan spool") {
		t.Fatalf("replayLocalCreateRecords() error = %v, want decode failure", err)
	}
}

func TestAddCreatePlanSizeClampsOverflow(t *testing.T) {
	if got := addCreatePlanSize(math.MaxInt64-1, 2); got != math.MaxInt64 {
		t.Fatalf("addCreatePlanSize() = %d, want MaxInt64", got)
	}
}

func TestBuildCreatePlanReusesLocalEntriesAfterMutation(t *testing.T) {
	root := t.TempDir()
	src := filepath.Join(root, "src")
	if err := os.MkdirAll(src, 0o755); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(src, "file.txt"), []byte("payload"), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	plan, err := (&Runner{}).buildCreatePlan(context.Background(), cli.Options{
		Members: []string{"src"},
		Chdir:   root,
	}, nil)
	if err != nil {
		t.Fatalf("buildCreatePlan() error = %v", err)
	}
	cleanupCreatePlan(t, plan)
	if !plan.totalKnown {
		t.Fatal("local-only plan should keep known total")
	}
	if plan.totalBytes != int64(len("payload")) {
		t.Fatalf("totalBytes = %d, want %d", plan.totalBytes, len("payload"))
	}

	if err := os.WriteFile(filepath.Join(src, "later.txt"), []byte("later"), 0o644); err != nil {
		t.Fatalf("WriteFile(later) error = %v", err)
	}

	var seen []string
	warnings, err := plannedCreateInputSource{plan: plan}.Visit(
		context.Background(),
		func(ref locator.Ref) error {
			t.Fatalf("unexpected s3 member: %+v", ref)
			return nil
		},
		func(source localCreateSource) (int, error) {
			err := source.Visit(context.Background(), func(record localCreateRecord, _ fs.FileInfo) error {
				seen = append(seen, record.archiveName)
				return nil
			})
			return 0, err
		},
	)
	if err != nil {
		t.Fatalf("Visit() error = %v", err)
	}
	if warnings != 0 {
		t.Fatalf("warnings = %d, want 0", warnings)
	}
	if got := strings.Join(seen, ","); got != "src,src/file.txt" {
		t.Fatalf("seen = %q, want %q", got, "src,src/file.txt")
	}
}

func TestBuildCreatePlanMixedMemberSizes(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "file.txt"), []byte("payload"), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	runner := newRunner(
		nil,
		fakeS3ArchiveStore{
			stat: func(_ context.Context, _ locator.Ref) (s3.Metadata, error) {
				return s3.Metadata{Size: 42}, nil
			},
		},
		nil,
		nil,
		nil,
	)

	plan, err := runner.buildCreatePlan(context.Background(), cli.Options{
		Members: []string{"file.txt", "s3://bucket/object.txt"},
		Chdir:   root,
	}, nil)
	if err != nil {
		t.Fatalf("buildCreatePlan() error = %v", err)
	}
	cleanupCreatePlan(t, plan)
	if !plan.totalKnown {
		t.Fatal("plan.totalKnown should remain true after successful scans")
	}
	if len(plan.members) != 2 {
		t.Fatalf("member count = %d, want 2", len(plan.members))
	}
	if plan.totalBytes != int64(len("payload"))+42 {
		t.Fatalf("totalBytes = %d, want %d", plan.totalBytes, int64(len("payload"))+42)
	}

	var sawLocal bool
	var sawS3 bool
	for _, member := range plan.members {
		switch member.ref.Kind {
		case locator.KindLocal:
			sawLocal = true
			if member.localPlanPath == "" || member.localRecordCount == 0 {
				t.Fatal("local member should keep planned records")
			}
		case locator.KindS3:
			sawS3 = true
			if member.ref.Key != "object.txt" {
				t.Fatalf("s3 key = %q, want %q", member.ref.Key, "object.txt")
			}
		default:
			t.Fatalf("unexpected member kind %q", member.ref.Kind)
		}
	}
	if !sawLocal || !sawS3 {
		t.Fatalf("sawLocal=%v sawS3=%v, want both true", sawLocal, sawS3)
	}
}

func TestBuildCreatePlanReturnsS3StatFailure(t *testing.T) {
	runner := newRunner(
		nil,
		fakeS3ArchiveStore{
			stat: func(_ context.Context, _ locator.Ref) (s3.Metadata, error) {
				return s3.Metadata{}, errors.New("head failed")
			},
		},
		nil,
		nil,
		nil,
	)

	_, err := runner.buildCreatePlan(context.Background(), cli.Options{
		Members: []string{"s3://bucket/object.txt"},
	}, nil)
	if err == nil || !strings.Contains(err.Error(), "head failed") {
		t.Fatalf("err = %v, want head failed", err)
	}
}

func TestBuildCreatePlanCancelsConcurrentTasksAfterFailure(t *testing.T) {
	started := make(chan struct{})
	cancelObserved := make(chan struct{})

	runner := newRunner(
		nil,
		fakeS3ArchiveStore{
			stat: func(ctx context.Context, ref locator.Ref) (s3.Metadata, error) {
				switch ref.Key {
				case "slow":
					close(started)
					<-ctx.Done()
					close(cancelObserved)
					return s3.Metadata{}, ctx.Err()
				case "fail":
					<-started
					return s3.Metadata{}, errors.New("stat failed")
				default:
					t.Fatalf("unexpected key %q", ref.Key)
					return s3.Metadata{}, nil
				}
			},
		},
		nil,
		nil,
		nil,
	)

	_, err := runner.buildCreatePlan(context.Background(), cli.Options{
		Members: []string{"s3://bucket/slow", "s3://bucket/fail"},
	}, nil)
	if err == nil || !strings.Contains(err.Error(), "stat failed") {
		t.Fatalf("err = %v, want stat failed", err)
	}

	select {
	case <-cancelObserved:
	case <-time.After(time.Second):
		t.Fatal("slow stat did not observe cancellation")
	}
}
