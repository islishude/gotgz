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
