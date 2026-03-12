package archivepath

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
		wantErr  string
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
			err := SafeSymlinkTarget(base, symlinkPath, tt.linkname, nil)
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

	err := EnsureSymlinkFreePath(base, filepath.Join(base, "dir", "file.txt"), nil)
	if err == nil || !strings.Contains(err.Error(), "follow symlink") {
		t.Fatalf("EnsureSymlinkFreePath() err = %v, want symlink traversal error", err)
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

	err := SafeSymlinkTarget(base, filepath.Join(base, "link"), "redir/file.txt", nil)
	if err == nil || !strings.Contains(err.Error(), "follow symlink") {
		t.Fatalf("SafeSymlinkTarget() err = %v, want symlink traversal error", err)
	}
}

func TestEnsureSymlinkFreePathCachesSafePrefixes(t *testing.T) {
	base := filepath.Join(t.TempDir(), "extract")
	targetDir := filepath.Join(base, "dir", "nested")
	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		t.Fatalf("mkdir targetDir: %v", err)
	}

	cache := NewPathSafetyCache()
	candidate := filepath.Join(targetDir, "file.txt")
	if err := EnsureSymlinkFreePath(base, candidate, cache); err != nil {
		t.Fatalf("EnsureSymlinkFreePath() error = %v", err)
	}

	for _, prefix := range []string{
		filepath.Join(base, "dir"),
		targetDir,
	} {
		if !cache.has(prefix) {
			t.Fatalf("expected cached prefix %q", prefix)
		}
	}
	if cache.has(candidate) {
		t.Fatalf("candidate leaf %q should not be cached when it does not exist", candidate)
	}
}

func TestEnsureSymlinkFreePathDoesNotCacheMissingPrefix(t *testing.T) {
	base := filepath.Join(t.TempDir(), "extract")
	outside := filepath.Join(t.TempDir(), "outside")
	if err := os.MkdirAll(base, 0o755); err != nil {
		t.Fatalf("mkdir base: %v", err)
	}
	if err := os.MkdirAll(outside, 0o755); err != nil {
		t.Fatalf("mkdir outside: %v", err)
	}

	cache := NewPathSafetyCache()
	missingCandidate := filepath.Join(base, "dir", "file.txt")
	if err := EnsureSymlinkFreePath(base, missingCandidate, cache); err != nil {
		t.Fatalf("EnsureSymlinkFreePath() initial error = %v", err)
	}
	if cache.has(filepath.Join(base, "dir")) {
		t.Fatalf("missing prefix %q should not be cached", filepath.Join(base, "dir"))
	}

	if err := os.Symlink(outside, filepath.Join(base, "dir")); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	err := EnsureSymlinkFreePath(base, filepath.Join(base, "dir", "later.txt"), cache)
	if err == nil || !strings.Contains(err.Error(), "follow symlink") {
		t.Fatalf("EnsureSymlinkFreePath() err = %v, want symlink traversal error", err)
	}
}
