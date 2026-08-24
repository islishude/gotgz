package local

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/islishude/gotgz/packages/locator"
)

// replaceStdin swaps os.Stdin for the duration of the test.
func replaceStdin(t *testing.T, f *os.File) {
	t.Helper()
	prev := os.Stdin
	os.Stdin = f
	t.Cleanup(func() {
		os.Stdin = prev
		if err := f.Close(); err != nil {
			t.Errorf("close stdin replacement: %v", err)
		}
	})
}

// replaceStdout swaps os.Stdout for the duration of the test.
func replaceStdout(t *testing.T, f *os.File) {
	t.Helper()
	prev := os.Stdout
	os.Stdout = f
	t.Cleanup(func() {
		os.Stdout = prev
		if err := f.Close(); err != nil {
			t.Errorf("close stdout replacement: %v", err)
		}
	})
}

// TestArchiveStoreOpenReaderLocal verifies that local files are opened and the
// size metadata is populated from the filesystem.
func TestArchiveStoreOpenReaderLocal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "archive.tar")
	want := []byte("archive payload")
	if err := os.WriteFile(path, want, 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	store := &ArchiveStore{}
	reader, meta, err := store.OpenReader(locator.Ref{Kind: locator.KindLocal, Path: path})
	if err != nil {
		t.Fatalf("OpenReader() error = %v", err)
	}
	defer func() {
		if err := reader.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	}()

	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if string(got) != string(want) {
		t.Fatalf("OpenReader() payload = %q, want %q", got, want)
	}
	if meta.Size != int64(len(want)) {
		t.Fatalf("OpenReader() size = %d, want %d", meta.Size, len(want))
	}
}

// TestArchiveStoreOpenReaderStdio verifies that stdio reads proxy through the
// current stdin stream.
func TestArchiveStoreOpenReaderStdio(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "stdin-*")
	if err != nil {
		t.Fatalf("CreateTemp() error = %v", err)
	}
	if _, err := f.WriteString("stdin payload"); err != nil {
		t.Fatalf("WriteString() error = %v", err)
	}
	if _, err := f.Seek(0, 0); err != nil {
		t.Fatalf("Seek() error = %v", err)
	}
	replaceStdin(t, f)

	store := &ArchiveStore{}
	reader, meta, err := store.OpenReader(locator.Ref{Kind: locator.KindStdio, Raw: "-"})
	if err != nil {
		t.Fatalf("OpenReader() error = %v", err)
	}
	defer func() {
		if err := reader.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	}()

	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if string(got) != "stdin payload" {
		t.Fatalf("OpenReader() payload = %q, want %q", got, "stdin payload")
	}
	if meta.Size != 0 {
		t.Fatalf("OpenReader() size = %d, want 0 for stdio", meta.Size)
	}
}

// TestArchiveStoreOpenReaderRejectsUnsupportedKind verifies that non-local,
// non-stdio archive references fail fast.
func TestArchiveStoreOpenReaderRejectsUnsupportedKind(t *testing.T) {
	store := &ArchiveStore{}
	if _, _, err := store.OpenReader(locator.Ref{Kind: locator.KindHTTP, Raw: "http://example.com/archive.tar"}); err == nil {
		t.Fatalf("OpenReader() error = nil, want non-nil")
	}
}

// TestArchiveStoreOpenWriterLocal verifies that local archive writes create the
// target file and persist the written data.
func TestArchiveStoreOpenWriterLocal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "archive.tar")
	store := &ArchiveStore{}

	writer, err := store.OpenWriter(locator.Ref{Kind: locator.KindLocal, Path: path})
	if err != nil {
		t.Fatalf("OpenWriter() error = %v", err)
	}
	if _, err := writer.Write([]byte("writer payload")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if string(got) != "writer payload" {
		t.Fatalf("OpenWriter() payload = %q, want %q", got, "writer payload")
	}
}

func TestArchiveStoreBeginWriterAbortPreservesExistingTarget(t *testing.T) {
	path := filepath.Join(t.TempDir(), "archive.tar")
	original := []byte("original")
	if err := os.WriteFile(path, original, 0o640); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	session, err := (&ArchiveStore{}).BeginWriter(locator.Ref{Kind: locator.KindLocal, Raw: path, Path: path})
	if err != nil {
		t.Fatalf("BeginWriter() error = %v", err)
	}
	if _, err := session.Write([]byte("replacement")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if err := session.Abort(errors.New("source failed")); err != nil {
		t.Fatalf("Abort() error = %v", err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if string(got) != string(original) {
		t.Fatalf("target = %q, want %q", got, original)
	}
}

func TestArchiveStoreBeginWriterCommitReplacesAndPreservesMode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "archive.tar")
	if err := os.WriteFile(path, []byte("original"), 0o640); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	session, err := (&ArchiveStore{}).BeginWriter(locator.Ref{Kind: locator.KindLocal, Raw: path, Path: path})
	if err != nil {
		t.Fatalf("BeginWriter() error = %v", err)
	}
	if _, err := session.Write([]byte("replacement")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if err := session.Commit(); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
	got, err := os.ReadFile(path)
	if err != nil || string(got) != "replacement" {
		t.Fatalf("target = %q, err=%v", got, err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat() error = %v", err)
	}
	if got := info.Mode().Perm(); got != 0o640 {
		t.Fatalf("mode = %o, want 640", got)
	}
}

func TestArchiveStoreBeginWriterRejectsSymlinkTarget(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "target")
	link := filepath.Join(root, "archive.tar")
	if err := os.WriteFile(target, []byte("payload"), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	if err := os.Symlink(target, link); err != nil {
		t.Fatalf("Symlink() error = %v", err)
	}
	if _, err := (&ArchiveStore{}).BeginWriter(locator.Ref{Kind: locator.KindLocal, Raw: link, Path: link}); err == nil {
		t.Fatal("BeginWriter() error = nil, want symlink rejection")
	}
}

// TestArchiveStoreOpenWriterStdio verifies that stdio writes proxy through the
// current stdout stream.
func TestArchiveStoreOpenWriterStdio(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "stdout-*")
	if err != nil {
		t.Fatalf("CreateTemp() error = %v", err)
	}
	replaceStdout(t, f)

	store := &ArchiveStore{}
	writer, err := store.OpenWriter(locator.Ref{Kind: locator.KindStdio, Raw: "-"})
	if err != nil {
		t.Fatalf("OpenWriter() error = %v", err)
	}
	if _, err := writer.Write([]byte("stdout payload")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := f.Sync(); err != nil {
		t.Fatalf("Sync() error = %v", err)
	}
	if _, err := f.Seek(0, 0); err != nil {
		t.Fatalf("Seek() error = %v", err)
	}

	got, err := io.ReadAll(f)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if string(got) != "stdout payload" {
		t.Fatalf("OpenWriter() payload = %q, want %q", got, "stdout payload")
	}
}

// TestArchiveStoreOpenWriterRejectsUnsupportedKind verifies that non-local,
// non-stdio write targets are rejected.
func TestArchiveStoreOpenWriterRejectsUnsupportedKind(t *testing.T) {
	store := &ArchiveStore{}
	if _, err := store.OpenWriter(locator.Ref{Kind: locator.KindHTTP, Raw: "http://example.com/archive.tar"}); err == nil {
		t.Fatalf("OpenWriter() error = nil, want non-nil")
	}
}
