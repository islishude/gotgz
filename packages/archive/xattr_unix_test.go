//go:build unix

package archive

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

// TestWritePathMetadataAndReadPathMetadataRoundTrip verifies that custom
// xattrs written to disk can be read back and ACL-like keys are classified.
func TestWritePathMetadataAndReadPathMetadataRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "payload.txt")
	if err := os.WriteFile(path, []byte("payload"), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	xattrs := map[string][]byte{
		"user.gotgz.test": []byte("value"),
		"user.gotgz.acl":  []byte("acl-value"),
	}
	if err := WritePathMetadata(path, xattrs, nil); err != nil {
		if errors.Is(err, unix.ENOTSUP) || errors.Is(err, unix.EPERM) {
			t.Skipf("filesystem does not allow xattrs: %v", err)
		}
		t.Fatalf("WritePathMetadata() error = %v", err)
	}

	gotXattrs, gotACLs, err := ReadPathMetadata(path)
	if err != nil {
		if errors.Is(err, unix.ENOTSUP) || errors.Is(err, unix.EPERM) {
			t.Skipf("filesystem does not allow xattrs: %v", err)
		}
		t.Fatalf("ReadPathMetadata() error = %v", err)
	}
	if !bytes.Equal(gotXattrs["user.gotgz.test"], xattrs["user.gotgz.test"]) {
		t.Fatalf("ReadPathMetadata() xattr test value = %q, want %q", gotXattrs["user.gotgz.test"], xattrs["user.gotgz.test"])
	}
	if !bytes.Equal(gotXattrs["user.gotgz.acl"], xattrs["user.gotgz.acl"]) {
		t.Fatalf("ReadPathMetadata() xattr acl value = %q, want %q", gotXattrs["user.gotgz.acl"], xattrs["user.gotgz.acl"])
	}
	if _, ok := gotACLs["user.gotgz.acl"]; ok {
		t.Fatal("ordinary xattr containing acl should not be classified as an ACL")
	}
}

// TestReadPathMetadataMissingPath verifies that missing paths surface the
// underlying filesystem error.
func TestReadPathMetadataMissingPath(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing")
	_, _, err := ReadPathMetadata(path)
	if err == nil {
		t.Fatalf("ReadPathMetadata() error = nil, want non-nil")
	}
	if !errors.Is(err, unix.ENOENT) {
		t.Fatalf("ReadPathMetadata() error = %v, want ENOENT", err)
	}
}

// TestWritePathMetadataMissingPath verifies that write failures are returned
// even when they come from joined xattr and ACL restoration errors.
func TestWritePathMetadataMissingPath(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing")
	err := WritePathMetadata(path, map[string][]byte{"user.gotgz.test": []byte("value")}, map[string][]byte{"user.gotgz.acl": []byte("acl")})
	if err == nil {
		t.Fatalf("WritePathMetadata() error = nil, want non-nil")
	}
	if !errors.Is(err, unix.ENOENT) {
		t.Fatalf("WritePathMetadata() error = %v, want ENOENT", err)
	}
}

func TestSymlinkMetadataDoesNotFollowTarget(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "target")
	link := filepath.Join(root, "link")
	if err := os.WriteFile(target, []byte("payload"), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	if err := os.Symlink("target", link); err != nil {
		t.Fatalf("Symlink() error = %v", err)
	}
	key := "user.gotgz.symlink-test"
	if err := unix.Setxattr(target, key, []byte("target-value"), 0); err != nil {
		if errors.Is(err, unix.ENOTSUP) || errors.Is(err, unix.EPERM) {
			t.Skipf("filesystem does not allow xattrs: %v", err)
		}
		t.Fatalf("Setxattr(target) error = %v", err)
	}

	linkAttrs, _, err := ReadPathMetadata(link)
	if err != nil && !errors.Is(err, unix.ENOTSUP) && !errors.Is(err, unix.EPERM) {
		t.Fatalf("ReadPathMetadata(link) error = %v", err)
	}
	if got := linkAttrs[key]; bytes.Equal(got, []byte("target-value")) {
		t.Fatalf("symlink metadata unexpectedly followed target: %q", got)
	}

	if err := WritePathMetadata(link, map[string][]byte{key: []byte("link-value")}, nil); err != nil {
		if errors.Is(err, unix.ENOTSUP) || errors.Is(err, unix.EPERM) {
			t.Skipf("filesystem does not allow symlink xattrs: %v", err)
		}
		t.Fatalf("WritePathMetadata(link) error = %v", err)
	}
	sz, err := unix.Getxattr(target, key, nil)
	if err != nil {
		t.Fatalf("Getxattr(target size) error = %v", err)
	}
	value := make([]byte, sz)
	if _, err := unix.Getxattr(target, key, value); err != nil {
		t.Fatalf("Getxattr(target) error = %v", err)
	}
	if string(value) != "target-value" {
		t.Fatalf("target xattr = %q, want target-value", value)
	}
}
