package engine

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSafeSymlinkTarget(t *testing.T) {
	base := filepath.Join(t.TempDir(), "extract")
	symlinkPath := filepath.Join(base, "sub", "link")

	tests := []struct {
		name     string
		linkname string
		wantErr  string // non-empty substring expected in error
	}{
		{name: "empty target", linkname: "", wantErr: "symlink target is empty"},
		{name: "relative within base", linkname: "../file.txt", wantErr: ""},
		{name: "relative escapes base", linkname: "../../../etc/passwd", wantErr: "escapes extraction directory"},
		{name: "absolute target", linkname: "/etc/passwd", wantErr: "absolute symlink target not allowed"},
		{name: "absolute target within tree", linkname: "/sub/other", wantErr: "absolute symlink target not allowed"},
		{name: "relative self-referencing", linkname: ".", wantErr: ""},
		{name: "relative deeper", linkname: "deeper/file", wantErr: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := safeSymlinkTarget(base, symlinkPath, tt.linkname)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("expected error containing %q, got: %v", tt.wantErr, err)
			}
		})
	}
}

func TestEnsureSymlinkFreePathRejectsExistingSymlink(t *testing.T) {
	base := filepath.Join(t.TempDir(), "extract")
	outside := filepath.Join(t.TempDir(), "outside")
	if err := os.MkdirAll(base, 0o755); err != nil {
		t.Fatalf("mkdir base: %v", err)
	}
	if err := os.MkdirAll(outside, 0o755); err != nil {
		t.Fatalf("mkdir outside: %v", err)
	}
	if err := os.Symlink(outside, filepath.Join(base, "dir")); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	err := ensureSymlinkFreePath(base, filepath.Join(base, "dir", "file.txt"))
	if err == nil || !strings.Contains(err.Error(), "follow symlink") {
		t.Fatalf("ensureSymlinkFreePath() err = %v, want symlink traversal error", err)
	}
}

func TestSafeSymlinkTargetRejectsResolvedSymlinkTraversal(t *testing.T) {
	base := filepath.Join(t.TempDir(), "extract")
	outside := filepath.Join(t.TempDir(), "outside")
	if err := os.MkdirAll(base, 0o755); err != nil {
		t.Fatalf("mkdir base: %v", err)
	}
	if err := os.MkdirAll(outside, 0o755); err != nil {
		t.Fatalf("mkdir outside: %v", err)
	}
	if err := os.Symlink(outside, filepath.Join(base, "redir")); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	err := safeSymlinkTarget(base, filepath.Join(base, "link"), "redir/file.txt")
	if err == nil || !strings.Contains(err.Error(), "follow symlink") {
		t.Fatalf("safeSymlinkTarget() err = %v, want symlink traversal error", err)
	}
}
