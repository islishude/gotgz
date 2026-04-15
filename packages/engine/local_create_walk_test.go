package engine

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/islishude/gotgz/packages/archivepath"
)

func TestWalkLocalCreateMember(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "dir", "skipme"), 0o755); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "dir", "file.txt"), []byte("ok"), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "dir", "skipme", "ignored.txt"), []byte("no"), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	var seen []string
	err := walkLocalCreateMember(context.Background(), "dir", root, archivepath.NewCompiledPathMatcher([]string{"dir/skipme"}), func(record localCreateRecord, _ fs.FileInfo) error {
		seen = append(seen, record.archiveName)
		return nil
	})
	if err != nil {
		t.Fatalf("walkLocalCreateMember() error = %v", err)
	}
	joined := strings.Join(seen, ",")
	if joined != "dir,dir/file.txt" {
		t.Fatalf("seen = %q, want %q", joined, "dir,dir/file.txt")
	}
}

func TestWalkLocalCreateMemberDotMember(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "file.txt"), []byte("ok"), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	var seen []string
	err := walkLocalCreateMember(context.Background(), ".", root, nil, func(record localCreateRecord, _ fs.FileInfo) error {
		seen = append(seen, record.archiveName)
		return nil
	})
	if err != nil {
		t.Fatalf("walkLocalCreateMember() error = %v", err)
	}
	joined := strings.Join(seen, ",")
	if joined != ".,file.txt" {
		t.Fatalf("seen = %q, want %q", joined, ".,file.txt")
	}
}

func TestWalkLocalCreateMemberUsesSymlinkMetadata(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "target.txt")
	link := filepath.Join(root, "link.txt")
	if err := os.WriteFile(target, []byte("ok"), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	if err := os.Symlink("target.txt", link); err != nil {
		t.Fatalf("Symlink() error = %v", err)
	}

	var linkMode fs.FileMode
	err := walkLocalCreateMember(context.Background(), ".", root, nil, func(record localCreateRecord, info fs.FileInfo) error {
		if record.archiveName == "link.txt" {
			linkMode = info.Mode()
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walkLocalCreateMember() error = %v", err)
	}
	if linkMode&os.ModeSymlink == 0 {
		t.Fatalf("link mode = %v, want symlink", linkMode)
	}
}

func TestCollectLocalCreateRecordsTotalsRegularFilesOnly(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "dir"), 0o755); err != nil {
		t.Fatalf("Mkdir() error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "file.txt"), []byte("payload"), 0o644); err != nil {
		t.Fatalf("WriteFile(file) error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "dir", "nested.txt"), []byte("nested"), 0o644); err != nil {
		t.Fatalf("WriteFile(nested) error = %v", err)
	}
	if err := os.Symlink("file.txt", filepath.Join(root, "link.txt")); err != nil {
		t.Fatalf("Symlink() error = %v", err)
	}

	records, total, err := collectLocalCreateRecords(context.Background(), ".", root, nil)
	if err != nil {
		t.Fatalf("collectLocalCreateRecords() error = %v", err)
	}
	if got, want := total, int64(len("payload")+len("nested")); got != want {
		t.Fatalf("total = %d, want %d", got, want)
	}
	if len(records) != 5 {
		t.Fatalf("record count = %d, want 5", len(records))
	}
}
