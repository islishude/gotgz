package engine

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
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
	err := walkLocalCreateMember(context.Background(), "dir", root, []string{"dir/skipme"}, func(entry localCreateEntry) error {
		seen = append(seen, entry.archiveName)
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
