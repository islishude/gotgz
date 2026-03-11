package engine

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/islishude/gotgz/internal/cli"
	"github.com/islishude/gotgz/internal/locator"
	"github.com/islishude/gotgz/internal/storage/s3"
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
	warnings, err := (&Runner{}).processCreatePlan(
		context.Background(),
		plan,
		func(ref locator.Ref) error {
			t.Fatalf("unexpected s3 member: %+v", ref)
			return nil
		},
		func(entries []localCreateEntry) (int, error) {
			for _, entry := range entries {
				seen = append(seen, entry.archiveName)
			}
			return 0, nil
		},
	)
	if err != nil {
		t.Fatalf("processCreatePlan() error = %v", err)
	}
	if warnings != 0 {
		t.Fatalf("warnings = %d, want 0", warnings)
	}
	if got := strings.Join(seen, ","); got != "src,src/file.txt" {
		t.Fatalf("seen = %q, want %q", got, "src,src/file.txt")
	}
}

func TestBuildCreatePlanContinuesLocalScanAfterS3StatFailure(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "file.txt"), []byte("payload"), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

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

	plan, err := runner.buildCreatePlan(context.Background(), cli.Options{
		Members: []string{"s3://bucket/object.txt", "file.txt"},
		Chdir:   root,
	}, nil)
	if err != nil {
		t.Fatalf("buildCreatePlan() error = %v", err)
	}
	if plan.totalKnown {
		t.Fatal("plan.totalKnown should be false after s3 stat failure")
	}
	if len(plan.members) != 2 {
		t.Fatalf("member count = %d, want 2", len(plan.members))
	}
	if len(plan.members[1].localEntries) == 0 {
		t.Fatal("local entries should still be scanned after s3 stat failure")
	}
}
