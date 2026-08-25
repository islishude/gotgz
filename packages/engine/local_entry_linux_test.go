//go:build linux

package engine

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/islishude/gotgz/packages/cli"
	"golang.org/x/sys/unix"
)

func TestOpenRegularLocalEntryRejectsSymlinkReplacement(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "member")
	if err := os.WriteFile(path, []byte("original"), 0o600); err != nil {
		t.Fatalf("WriteFile(original) error = %v", err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatalf("Remove(original) error = %v", err)
	}
	if err := os.Symlink("target", path); err != nil {
		t.Fatalf("Symlink(replacement) error = %v", err)
	}

	_, err := openLocalEntry(localCreateRecord{current: path, archiveName: "member"}, fs.FileMode(0))
	if err == nil || !errors.Is(err, unix.ELOOP) {
		t.Fatalf("openLocalEntry() error = %v, want O_NOFOLLOW ELOOP", err)
	}
}

func TestOpenRegularLocalEntryUsesSameInodeForHeaderAndPayload(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "member")
	if err := os.WriteFile(path, []byte("scanned"), 0o600); err != nil {
		t.Fatalf("WriteFile(scanned) error = %v", err)
	}
	replacement := filepath.Join(root, "replacement")
	if err := os.WriteFile(replacement, []byte("opened replacement"), 0o640); err != nil {
		t.Fatalf("WriteFile(replacement) error = %v", err)
	}
	if err := os.Rename(replacement, path); err != nil {
		t.Fatalf("Rename(replacement) error = %v", err)
	}

	entry, err := openLocalEntry(localCreateRecord{current: path, archiveName: "member"}, fs.FileMode(0))
	if err != nil {
		t.Fatalf("openLocalEntry() error = %v", err)
	}
	defer func() { _ = entry.Close() }()
	later := filepath.Join(root, "later")
	if err := os.WriteFile(later, []byte("later path contents are different"), 0o600); err != nil {
		t.Fatalf("WriteFile(later) error = %v", err)
	}
	if err := os.Rename(later, path); err != nil {
		t.Fatalf("Rename(later) error = %v", err)
	}

	writer := &recordingTarWriter{}
	if _, err := (&Runner{}).writeLocalTarRecord(context.Background(), writer, entry, false, MetadataPolicy{}, nil); err != nil {
		t.Fatalf("writeLocalTarRecord() error = %v", err)
	}
	if len(writer.headers) != 1 || writer.headers[0].Size != int64(len("opened replacement")) {
		t.Fatalf("header = %+v, want opened replacement size", writer.headers)
	}
	if len(writer.bodies) != 1 || string(writer.bodies[0]) != "opened replacement" {
		t.Fatalf("payload = %q, want opened replacement", writer.bodies)
	}
}

func TestOpenRegularLocalEntryKeepsZipHeaderAndPayloadOnSameInode(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "member")
	if err := os.WriteFile(path, []byte("opened zip payload"), 0o600); err != nil {
		t.Fatalf("WriteFile(member) error = %v", err)
	}
	entry, err := openLocalEntry(localCreateRecord{current: path, archiveName: "member"}, fs.FileMode(0))
	if err != nil {
		t.Fatalf("openLocalEntry() error = %v", err)
	}
	defer func() { _ = entry.Close() }()
	later := filepath.Join(root, "later")
	if err := os.WriteFile(later, []byte("later zip path contents"), 0o600); err != nil {
		t.Fatalf("WriteFile(later) error = %v", err)
	}
	if err := os.Rename(later, path); err != nil {
		t.Fatalf("Rename(later) error = %v", err)
	}

	writer := &recordingZipWriter{}
	if _, err := (&Runner{}).writeLocalZipRecord(context.Background(), writer, entry, false, nil); err != nil {
		t.Fatalf("writeLocalZipRecord() error = %v", err)
	}
	if len(writer.headers) != 1 || writer.headers[0].UncompressedSize64 != uint64(len("opened zip payload")) {
		t.Fatalf("header = %+v, want opened payload size", writer.headers)
	}
	if len(writer.bodies) != 1 || string(writer.bodies[0]) != "opened zip payload" {
		t.Fatalf("payload = %q, want opened zip payload", writer.bodies)
	}
}

func TestOpenRegularLocalEntryRejectsSpecialFileReplacementWithoutBlocking(t *testing.T) {
	path := filepath.Join(t.TempDir(), "member")
	if err := os.WriteFile(path, []byte("original"), 0o600); err != nil {
		t.Fatalf("WriteFile(original) error = %v", err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatalf("Remove(original) error = %v", err)
	}
	if err := unix.Mkfifo(path, 0o600); err != nil {
		t.Fatalf("Mkfifo(replacement) error = %v", err)
	}
	if _, err := openLocalEntry(localCreateRecord{current: path, archiveName: "member"}, fs.FileMode(0)); err == nil || !strings.Contains(err.Error(), "changed from a regular file") {
		t.Fatalf("openLocalEntry() error = %v, want special-file type drift", err)
	}
}

func TestPlannedLocalCreateSourceClosesFastPathDescriptor(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "member")
	if err := os.WriteFile(path, []byte("payload"), 0o600); err != nil {
		t.Fatalf("WriteFile(member) error = %v", err)
	}
	plan, err := (&Runner{}).buildCreatePlan(context.Background(), cli.Options{Members: []string{"member"}, Chdir: root})
	if err != nil {
		t.Fatalf("buildCreatePlan() error = %v", err)
	}
	defer func() { _ = plan.Close() }()
	var opened *os.File
	err = (plannedLocalCreateSource{planPath: plan.members[0].localPlanPath}).Visit(context.Background(), func(entry *localEntryHandle) error {
		opened = entry.file
		return nil
	})
	if err != nil {
		t.Fatalf("Visit() error = %v", err)
	}
	if opened == nil {
		t.Fatal("regular entry did not use the Linux fast-path descriptor")
	}
	if _, err := opened.Read(make([]byte, 1)); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("Read(after Visit) error = %v, want os.ErrClosed", err)
	}
}
