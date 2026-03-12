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

func TestEnsureSymlinkFreePathAllowsBaseItself(t *testing.T) {
	base := filepath.Join(t.TempDir(), "extract")
	if err := os.MkdirAll(base, 0o755); err != nil {
		t.Fatalf("mkdir base: %v", err)
	}

	if err := EnsureSymlinkFreePath(base, base, nil); err != nil {
		t.Fatalf("EnsureSymlinkFreePath(base, base) error = %v", err)
	}
}

func TestEnsureSymlinkFreePathRejectsOutsideBase(t *testing.T) {
	root := t.TempDir()
	base := filepath.Join(root, "extract")
	outside := filepath.Join(root, "outside", "file.txt")
	if err := os.MkdirAll(base, 0o755); err != nil {
		t.Fatalf("mkdir base: %v", err)
	}

	err := EnsureSymlinkFreePath(base, outside, nil)
	if err == nil {
		t.Fatal("expected outside-base error")
	}
	if !strings.Contains(err.Error(), "outside target directory") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestStripPathComponents(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		count    int
		want     string
		wantKept bool
	}{
		{name: "no strip", input: "parent/dir/file.txt", count: 0, want: "parent/dir/file.txt", wantKept: true},
		{name: "strip one", input: "parent/dir/file.txt", count: 1, want: "dir/file.txt", wantKept: true},
		{name: "strip all drops", input: "parent/file.txt", count: 2, want: "", wantKept: false},
		{name: "leading slash cleaned", input: "/parent/file.txt", count: 1, want: "file.txt", wantKept: true},
		{name: "dot path drops", input: ".", count: 1, want: "", wantKept: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, kept := StripPathComponents(tt.input, tt.count)
			if kept != tt.wantKept {
				t.Fatalf("StripPathComponents(%q, %d) kept = %v, want %v", tt.input, tt.count, kept, tt.wantKept)
			}
			if got != tt.want {
				t.Fatalf("StripPathComponents(%q, %d) got = %q, want %q", tt.input, tt.count, got, tt.want)
			}
		})
	}
}

func TestSafeJoin(t *testing.T) {
	t.Run("normal member", func(t *testing.T) {
		got, err := SafeJoin("/tmp/out", "dir/file.txt")
		if err != nil {
			t.Fatalf("SafeJoin() error = %v", err)
		}
		want := filepath.Clean("/tmp/out/dir/file.txt")
		if got != want {
			t.Fatalf("SafeJoin() = %q, want %q", got, want)
		}
	})

	t.Run("traversal blocked", func(t *testing.T) {
		_, err := SafeJoin("/tmp/out", "../../etc/passwd")
		if err == nil {
			t.Fatal("expected traversal error")
		}
		if !strings.Contains(err.Error(), "outside target directory") {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestEnsureSymlinkFreeParentPath(t *testing.T) {
	base := filepath.Join(t.TempDir(), "extract")
	outside := filepath.Join(t.TempDir(), "outside")
	if err := os.MkdirAll(base, 0o755); err != nil {
		t.Fatalf("mkdir base: %v", err)
	}
	if err := os.MkdirAll(outside, 0o755); err != nil {
		t.Fatalf("mkdir outside: %v", err)
	}

	parent := filepath.Join(base, "dir")
	if err := os.Symlink(outside, parent); err != nil {
		t.Fatalf("symlink parent: %v", err)
	}

	err := EnsureSymlinkFreeParentPath(base, filepath.Join(parent, "file.txt"), nil)
	if err == nil || !strings.Contains(err.Error(), "follow symlink") {
		t.Fatalf("EnsureSymlinkFreeParentPath() err = %v, want symlink traversal error", err)
	}
}

func TestPathSafetyCacheInvalidate(t *testing.T) {
	base := filepath.Join(t.TempDir(), "extract")
	cache := NewPathSafetyCache()
	dir := filepath.Join(base, "dir")
	nested := filepath.Join(base, "dir", "nested")
	other := filepath.Join(base, "other")

	cache.add(dir)
	cache.add(nested)
	cache.add(other)

	cache.Invalidate(dir)

	if cache.has(dir) {
		t.Fatalf("expected %q to be invalidated", dir)
	}
	if cache.has(nested) {
		t.Fatalf("expected descendant %q to be invalidated", nested)
	}
	if !cache.has(other) {
		t.Fatalf("expected unrelated prefix %q to remain cached", other)
	}
}

func TestPathSafetyCacheNilMethods(t *testing.T) {
	var cache *PathSafetyCache
	cache.add("/tmp/x")
	cache.Invalidate("/tmp/x")
	if cache.has("/tmp/x") {
		t.Fatal("nil cache should not report cached paths")
	}
}

func TestEnsureSymlinkFreeParentPathParentEqualsCandidate(t *testing.T) {
	base := t.TempDir()
	if err := EnsureSymlinkFreeParentPath(base, string(filepath.Separator), nil); err != nil {
		t.Fatalf("EnsureSymlinkFreeParentPath() error = %v", err)
	}
}

func TestEnsureSymlinkFreePathUsesCachedPrefix(t *testing.T) {
	base := filepath.Join(t.TempDir(), "extract")
	outside := filepath.Join(t.TempDir(), "outside")
	if err := os.MkdirAll(base, 0o755); err != nil {
		t.Fatalf("mkdir base: %v", err)
	}
	if err := os.MkdirAll(outside, 0o755); err != nil {
		t.Fatalf("mkdir outside: %v", err)
	}
	if err := os.Symlink(outside, filepath.Join(base, "cached")); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	cache := NewPathSafetyCache()
	cache.add(filepath.Join(base, "cached"))
	if err := EnsureSymlinkFreePath(base, filepath.Join(base, "cached", "file.txt"), cache); err != nil {
		t.Fatalf("EnsureSymlinkFreePath() with cached prefix error = %v", err)
	}
}

func TestEnsureSymlinkFreePathLstatError(t *testing.T) {
	base := filepath.Join(t.TempDir(), "extract")
	if err := os.MkdirAll(base, 0o755); err != nil {
		t.Fatalf("mkdir base: %v", err)
	}

	err := EnsureSymlinkFreePath(base, filepath.Join(base, "bad\x00name", "file.txt"), nil)
	if err == nil {
		t.Fatal("expected lstat error")
	}
	if !strings.Contains(err.Error(), "lstat") {
		t.Fatalf("unexpected error: %v", err)
	}
}
