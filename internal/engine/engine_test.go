package engine

import (
	"path/filepath"
	"testing"

	"github.com/islishude/gotgz/internal/cli"
)

func TestResolvePolicyOverrides(t *testing.T) {
	so := false
	sp := true
	p := resolvePolicy(cli.Options{SameOwner: &so, SamePermissions: &sp, NumericOwner: true})
	if p.SameOwner {
		t.Fatalf("SameOwner should be false")
	}
	if !p.SamePerms {
		t.Fatalf("SamePerms should be true")
	}
	if !p.NumericOwner {
		t.Fatalf("NumericOwner should be true")
	}
}

func TestSafeJoinBlocksTraversal(t *testing.T) {
	_, err := safeJoin("/tmp/out", "../../etc/passwd")
	if err == nil {
		t.Fatalf("expected traversal error")
	}
}

func TestSafeJoinNormal(t *testing.T) {
	p, err := safeJoin("/tmp/out", "dir/file.txt")
	if err != nil {
		t.Fatalf("safeJoin error = %v", err)
	}
	want := filepath.Clean("/tmp/out/dir/file.txt")
	if p != want {
		t.Fatalf("path = %q, want %q", p, want)
	}
}
