//go:build unix

package local

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/islishude/gotgz/packages/archive"
	"golang.org/x/sys/unix"
)

func TestArchiveStoreCommitPreservesOwnerGroupAndXattrs(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "archive.tar")
	if err := os.WriteFile(path, []byte("original"), 0o640); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	key := "user.gotgz.atomic-preserve"
	value := []byte("metadata")
	if err := archive.WritePathMetadata(path, map[string][]byte{key: value}, nil); err != nil {
		if errors.Is(err, unix.ENOTSUP) || errors.Is(err, unix.EPERM) {
			t.Skipf("filesystem does not support test xattr: %v", err)
		}
		t.Fatalf("WritePathMetadata() error = %v", err)
	}
	beforeInfo, err := os.Lstat(path)
	if err != nil {
		t.Fatalf("Lstat(before) error = %v", err)
	}
	beforePlatform, err := inspectPlatformTargetMetadata(beforeInfo)
	if err != nil {
		t.Fatalf("inspectPlatformTargetMetadata(before) error = %v", err)
	}

	session, err := beginLocalFileWrite(path)
	if err != nil {
		t.Fatalf("beginLocalFileWrite() error = %v", err)
	}
	if _, err := session.Write([]byte("replacement")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if err := session.Commit(); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
	afterInfo, err := os.Lstat(path)
	if err != nil {
		t.Fatalf("Lstat(after) error = %v", err)
	}
	afterPlatform, err := inspectPlatformTargetMetadata(afterInfo)
	if err != nil {
		t.Fatalf("inspectPlatformTargetMetadata(after) error = %v", err)
	}
	if afterPlatform.uid != beforePlatform.uid || afterPlatform.gid != beforePlatform.gid {
		t.Fatalf("owner/group = %d:%d, want %d:%d", afterPlatform.uid, afterPlatform.gid, beforePlatform.uid, beforePlatform.gid)
	}
	xattrs, _, err := archive.ReadPathMetadata(path)
	if err != nil {
		t.Fatalf("ReadPathMetadata() error = %v", err)
	}
	if !bytes.Equal(xattrs[key], value) {
		t.Fatalf("xattr %q = %q, want %q", key, xattrs[key], value)
	}
}
