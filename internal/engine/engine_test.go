package engine

import (
	"archive/tar"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/islishude/gotgz/internal/archive"
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

func TestStripPathComponents(t *testing.T) {
	got, ok := stripPathComponents("parent/dir/file.txt", 1)
	if !ok {
		t.Fatalf("expected keep")
	}
	if got != "dir/file.txt" {
		t.Fatalf("got %q, want %q", got, "dir/file.txt")
	}
}

func TestStripPathComponentsDrop(t *testing.T) {
	_, ok := stripPathComponents("parent/file.txt", 2)
	if ok {
		t.Fatalf("expected drop")
	}
}

func TestFilterACLLikeXattrs(t *testing.T) {
	in := map[string][]byte{
		"user.mime_type":             []byte("text/plain"),
		"system.posix_acl_access":    []byte("acl-a"),
		"system.posix_acl_default":   []byte("acl-b"),
		"trusted.gotgz.custom-field": []byte("v"),
	}
	got := filterACLLikeXattrs(in)
	if _, ok := got["system.posix_acl_access"]; ok {
		t.Fatalf("acl access xattr should be filtered")
	}
	if _, ok := got["system.posix_acl_default"]; ok {
		t.Fatalf("acl default xattr should be filtered")
	}
	if string(got["user.mime_type"]) != "text/plain" {
		t.Fatalf("non-acl xattr should remain")
	}
	if string(got["trusted.gotgz.custom-field"]) != "v" {
		t.Fatalf("non-acl xattr should remain")
	}
}

func TestResolveMetadataPolicy(t *testing.T) {
	p := resolveMetadataPolicy(cli.Options{Xattrs: true, ACL: false})
	if !p.Xattrs {
		t.Fatalf("Xattrs should be true")
	}
	if p.ACL {
		t.Fatalf("ACL should be false")
	}
}

func TestPrepareMetadataForArchive(t *testing.T) {
	xattrs := map[string][]byte{
		"user.mime_type":           []byte("text/plain"),
		"system.posix_acl_access":  []byte("acl-a"),
		"system.posix_acl_default": []byte("acl-b"),
	}
	acls := map[string][]byte{
		"system.posix_acl_access": []byte("acl-a"),
	}

	t.Run("all disabled", func(t *testing.T) {
		gotX, gotA := prepareMetadataForArchive(xattrs, acls, MetadataPolicy{})
		if gotX != nil {
			t.Fatalf("xattrs expected nil")
		}
		if gotA != nil {
			t.Fatalf("acls expected nil")
		}
	})

	t.Run("xattrs enabled acl disabled", func(t *testing.T) {
		gotX, gotA := prepareMetadataForArchive(xattrs, acls, MetadataPolicy{Xattrs: true, ACL: false})
		if gotA != nil {
			t.Fatalf("acls expected nil")
		}
		if _, ok := gotX["system.posix_acl_access"]; ok {
			t.Fatalf("acl-like xattrs should be filtered")
		}
		if string(gotX["user.mime_type"]) != "text/plain" {
			t.Fatalf("non-acl xattrs should remain")
		}
	})

	t.Run("xattrs disabled acl enabled", func(t *testing.T) {
		gotX, gotA := prepareMetadataForArchive(xattrs, acls, MetadataPolicy{Xattrs: false, ACL: true})
		if gotX != nil {
			t.Fatalf("xattrs expected nil")
		}
		if string(gotA["system.posix_acl_access"]) != "acl-a" {
			t.Fatalf("acls should remain")
		}
	})
}

func TestDecodeMetadataForExtract(t *testing.T) {
	hdr := &tar.Header{Name: "a.txt", Mode: 0o644}
	archive.EncodeXattrToPAX(hdr, map[string][]byte{
		"user.mime_type":          []byte("text/plain"),
		"system.posix_acl_access": []byte("acl-xattr"),
	})
	archive.EncodeACLToPAX(hdr, map[string][]byte{"system.posix_acl_access": []byte("acl-record")})

	t.Run("all disabled", func(t *testing.T) {
		x, a := decodeMetadataForExtract(hdr, MetadataPolicy{})
		if x != nil || a != nil {
			t.Fatalf("expected nil metadata")
		}
	})

	t.Run("xattrs enabled acl disabled", func(t *testing.T) {
		x, a := decodeMetadataForExtract(hdr, MetadataPolicy{Xattrs: true})
		if a != nil {
			t.Fatalf("acls expected nil")
		}
		if _, ok := x["system.posix_acl_access"]; ok {
			t.Fatalf("acl-like xattr should be filtered")
		}
		if string(x["user.mime_type"]) != "text/plain" {
			t.Fatalf("expected regular xattr")
		}
	})

	t.Run("acl enabled", func(t *testing.T) {
		x, a := decodeMetadataForExtract(hdr, MetadataPolicy{ACL: true})
		if x != nil {
			t.Fatalf("xattrs expected nil when --xattrs not set")
		}
		if string(a["system.posix_acl_access"]) != "acl-record" {
			t.Fatalf("expected acl record")
		}
	})
}

func TestLoadExcludePatternsRejectsInvalidInlinePattern(t *testing.T) {
	_, err := loadExcludePatterns([]string{"["}, nil)
	if err == nil {
		t.Fatalf("expected invalid pattern error")
	}
	if !strings.Contains(err.Error(), "invalid exclude pattern") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoadExcludePatternsRejectsInvalidPatternInFile(t *testing.T) {
	tmpDir := t.TempDir()
	patternsFile := filepath.Join(tmpDir, "exclude.txt")
	if err := os.WriteFile(patternsFile, []byte("*.tmp\n[\n"), 0o600); err != nil {
		t.Fatalf("write patterns file: %v", err)
	}

	_, err := loadExcludePatterns(nil, []string{patternsFile})
	if err == nil {
		t.Fatalf("expected invalid pattern error")
	}
	if !strings.Contains(err.Error(), "exclude.txt:2") {
		t.Fatalf("error should include file and line: %v", err)
	}
}
