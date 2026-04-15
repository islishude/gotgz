package engine

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/islishude/gotgz/packages/cli"
	"github.com/islishude/gotgz/packages/locator"
	"github.com/islishude/gotgz/packages/storage/s3"
)

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
			if len(member.localRecords) == 0 {
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
