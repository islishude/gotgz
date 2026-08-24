package engine

import (
	"bytes"
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/islishude/gotgz/packages/archive"
	"github.com/islishude/gotgz/packages/archivepath"
	"github.com/islishude/gotgz/packages/archiveprogress"
	"github.com/islishude/gotgz/packages/cli"
)

func TestComputeExtractPerm(t *testing.T) {
	got := computeExtractPerm(0, 0o755, false)
	want := fs.FileMode(0o755) &^ archive.CurrentUmask()
	if got != want {
		t.Fatalf("computeExtractPerm() = %#o, want %#o", got, want)
	}

	got = computeExtractPerm(0o640, 0o755, true)
	if got != 0o640 {
		t.Fatalf("computeExtractPerm() = %#o, want %#o", got, fs.FileMode(0o640))
	}
}

func TestWriteLocalRegularTarget(t *testing.T) {
	base := t.TempDir()
	target := filepath.Join(base, "nested", "file.txt")
	if err := writeLocalRegularTarget(context.Background(), base, target, 0o640, strings.NewReader("payload"), nil); err != nil {
		t.Fatalf("writeLocalRegularTarget() error = %v", err)
	}

	data, err := os.ReadFile(target)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if string(data) != "payload" {
		t.Fatalf("payload = %q, want %q", string(data), "payload")
	}
	info, err := os.Stat(target)
	if err != nil {
		t.Fatalf("Stat() error = %v", err)
	}
	if info.Mode().Perm() != 0o640 {
		t.Fatalf("perm = %#o, want %#o", info.Mode().Perm(), fs.FileMode(0o640))
	}
}

func TestReplaceLocalSymlinkTarget(t *testing.T) {
	base := t.TempDir()
	if err := os.WriteFile(filepath.Join(base, "target.txt"), []byte("ok"), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	linkPath := filepath.Join(base, "links", "item")
	if err := os.MkdirAll(filepath.Dir(linkPath), 0o755); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}
	if err := os.WriteFile(linkPath, []byte("old"), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	if err := replaceLocalSymlinkTarget(base, linkPath, "../target.txt", nil); err != nil {
		t.Fatalf("replaceLocalSymlinkTarget() error = %v", err)
	}
	resolved, err := os.Readlink(linkPath)
	if err != nil {
		t.Fatalf("Readlink() error = %v", err)
	}
	if resolved != "../target.txt" {
		t.Fatalf("link target = %q, want %q", resolved, "../target.txt")
	}
}

func TestReplaceLocalSymlinkTargetInvalidatesCachedPrefixes(t *testing.T) {
	base := t.TempDir()
	cache := archivepath.NewPathSafetyCache()
	oldDir := filepath.Join(base, "dir")
	if err := os.MkdirAll(oldDir, 0o755); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}
	if err := os.MkdirAll(filepath.Join(base, "elsewhere"), 0o755); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}

	if err := archivepath.EnsureSymlinkFreePath(base, filepath.Join(oldDir, "file.txt"), cache); err != nil {
		t.Fatalf("EnsureSymlinkFreePath() error = %v", err)
	}

	if err := replaceLocalSymlinkTarget(base, oldDir, "elsewhere", cache); err != nil {
		t.Fatalf("replaceLocalSymlinkTarget() error = %v", err)
	}
	err := archivepath.EnsureSymlinkFreePath(base, filepath.Join(oldDir, "child.txt"), cache)
	if err == nil || !strings.Contains(err.Error(), "follow symlink") {
		t.Fatalf("EnsureSymlinkFreePath() err = %v, want symlink traversal error", err)
	}
}

func TestLocalMetadataSessionReportsWarningsAndContinues(t *testing.T) {
	var stderr bytes.Buffer
	runner := &Runner{stderr: &stderr}
	reporter := archiveprogress.NewReporter(&stderr, cli.ProgressNever, 0, false, time.Now(), false)
	session := newLocalMetadataSession(runner, reporter)
	warnings := session.apply(localMetadataRecord{
		target:      filepath.Join(t.TempDir(), "missing", "file"),
		archiveName: "missing/file",
		perm:        0o600,
		modTime:     time.Unix(1_700_000_000, 0),
	})
	if warnings < 2 {
		t.Fatalf("warnings = %d, want permission and timestamp warnings", warnings)
	}
	if !strings.Contains(stderr.String(), "permissions") || !strings.Contains(stderr.String(), "timestamp") {
		t.Fatalf("stderr = %q", stderr.String())
	}
}
