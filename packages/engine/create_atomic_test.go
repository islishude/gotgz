package engine

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/islishude/gotgz/packages/cli"
	"github.com/islishude/gotgz/packages/locator"
	localstore "github.com/islishude/gotgz/packages/storage/local"
)

type removeSourceOnBeginStore struct {
	*localstore.ArchiveStore
	source string
}

type abortFailureWriteSession struct {
	abortErr error
}

func (*abortFailureWriteSession) Write(p []byte) (int, error) { return len(p), nil }
func (*abortFailureWriteSession) Close() error                { return nil }
func (*abortFailureWriteSession) Commit() error               { return nil }
func (s *abortFailureWriteSession) Abort(error) error         { return s.abortErr }

type removeSourceAndFailAbortStore struct {
	*localstore.ArchiveStore
	source   string
	abortErr error
}

func (s *removeSourceAndFailAbortStore) BeginWriter(locator.Ref) (localstore.WriteSession, error) {
	if err := os.Remove(s.source); err != nil {
		return nil, err
	}
	return &abortFailureWriteSession{abortErr: s.abortErr}, nil
}

func (s *removeSourceOnBeginStore) BeginWriter(ref locator.Ref) (localstore.WriteSession, error) {
	if err := os.Remove(s.source); err != nil {
		return nil, err
	}
	return s.ArchiveStore.BeginWriter(ref)
}

func TestCreateMissingMemberPreservesExistingArchive(t *testing.T) {
	root := t.TempDir()
	archivePath := filepath.Join(root, "existing.tar")
	original := []byte("existing archive bytes")
	if err := os.WriteFile(archivePath, original, 0o640); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	runner := newRunner(&localstore.ArchiveStore{}, nil, nil, io.Discard, io.Discard)
	result := runner.Run(context.Background(), cli.Options{
		Mode:     cli.ModeCreate,
		Archive:  archivePath,
		Chdir:    root,
		Members:  []string{"missing"},
		Progress: cli.ProgressNever,
	})
	if result.ExitCode != ExitFatal || result.Err == nil {
		t.Fatalf("Run() = %+v, want fatal missing-member error", result)
	}
	got, err := os.ReadFile(archivePath)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if !bytes.Equal(got, original) {
		t.Fatalf("archive changed after failed create: got %q, want %q", got, original)
	}
	if matches, err := filepath.Glob(filepath.Join(root, ".existing.tar.gotgz-*")); err != nil || len(matches) != 0 {
		t.Fatalf("temporary files = %v, err=%v", matches, err)
	}
}

func TestCreateSourceFailureAfterPreflightAbortsTransaction(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source.txt")
	archivePath := filepath.Join(root, "existing.tar")
	if err := os.WriteFile(source, []byte("source"), 0o644); err != nil {
		t.Fatalf("WriteFile(source) error = %v", err)
	}
	original := []byte("existing archive bytes")
	if err := os.WriteFile(archivePath, original, 0o640); err != nil {
		t.Fatalf("WriteFile(archive) error = %v", err)
	}

	store := &removeSourceOnBeginStore{ArchiveStore: &localstore.ArchiveStore{}, source: source}
	runner := newRunner(store, nil, nil, io.Discard, io.Discard)
	result := runner.Run(context.Background(), cli.Options{Mode: cli.ModeCreate, Archive: archivePath, Chdir: root, Members: []string{"source.txt"}})
	if result.ExitCode != ExitFatal || result.Err == nil {
		t.Fatalf("Run() = %+v, want fatal post-preflight source error", result)
	}
	got, err := os.ReadFile(archivePath)
	if err != nil || !bytes.Equal(got, original) {
		t.Fatalf("archive = %q, err=%v, want original", got, err)
	}
}

func TestCreateRejectsExactOutputInputOverlapWithoutTruncation(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "data")
	original := []byte("source payload")
	if err := os.WriteFile(path, original, 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	runner := newRunner(&localstore.ArchiveStore{}, nil, nil, io.Discard, io.Discard)
	result := runner.Run(context.Background(), cli.Options{Mode: cli.ModeCreate, Archive: path, Chdir: root, Members: []string{"data"}})
	if result.ExitCode != ExitFatal || result.Err == nil || !strings.Contains(result.Err.Error(), "cannot also be an input") {
		t.Fatalf("Run() = %+v, want overlap error", result)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if !bytes.Equal(got, original) {
		t.Fatalf("source changed after overlap rejection: got %q, want %q", got, original)
	}
}

func TestCreateSkipsExistingOutputInsideInputTree(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "payload.txt"), []byte("payload"), 0o644); err != nil {
		t.Fatalf("WriteFile(payload) error = %v", err)
	}
	archivePath := filepath.Join(root, "bundle.tar")
	if err := os.WriteFile(archivePath, []byte("old archive"), 0o644); err != nil {
		t.Fatalf("WriteFile(archive) error = %v", err)
	}

	runner := newRunner(&localstore.ArchiveStore{}, nil, nil, io.Discard, io.Discard)
	result := runner.Run(context.Background(), cli.Options{Mode: cli.ModeCreate, Archive: archivePath, Chdir: root, Members: []string{"."}})
	if result.ExitCode != ExitWarning || result.Err != nil {
		t.Fatalf("Run() = %+v, want successful create with output-skip warning", result)
	}

	var listed bytes.Buffer
	listRunner := newRunner(&localstore.ArchiveStore{}, nil, nil, &listed, io.Discard)
	if result := listRunner.Run(context.Background(), cli.Options{Mode: cli.ModeList, Archive: archivePath}); result.ExitCode != ExitSuccess {
		t.Fatalf("list Run() = %+v", result)
	}
	if strings.Contains(listed.String(), "bundle.tar") || !strings.Contains(listed.String(), "payload.txt") {
		t.Fatalf("archive members = %q, want payload without output archive", listed.String())
	}
}

func TestCreateIncludesLegitimateTempLikeInputName(t *testing.T) {
	root := t.TempDir()
	hiddenName := ".bundle.tar.gotgz-backup"
	if err := os.WriteFile(filepath.Join(root, hiddenName), []byte("payload"), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	archivePath := filepath.Join(root, "bundle.tar")
	runner := newRunner(&localstore.ArchiveStore{}, nil, nil, io.Discard, io.Discard)
	if result := runner.Run(context.Background(), cli.Options{Mode: cli.ModeCreate, Archive: archivePath, Chdir: root, Members: []string{"."}}); result.ExitCode != ExitSuccess {
		t.Fatalf("create Run() = %+v", result)
	}
	var listed bytes.Buffer
	listRunner := newRunner(&localstore.ArchiveStore{}, nil, nil, &listed, io.Discard)
	if result := listRunner.Run(context.Background(), cli.Options{Mode: cli.ModeList, Archive: archivePath}); result.ExitCode != ExitSuccess {
		t.Fatalf("list Run() = %+v", result)
	}
	if !strings.Contains(listed.String(), hiddenName) {
		t.Fatalf("archive members = %q, want %s", listed.String(), hiddenName)
	}
}

func TestCreateJoinsAbortCleanupFailure(t *testing.T) {
	for _, archiveName := range []string{"archive.tar", "archive.zip"} {
		t.Run(archiveName, func(t *testing.T) {
			root := t.TempDir()
			source := filepath.Join(root, "source.txt")
			if err := os.WriteFile(source, []byte("payload"), 0o644); err != nil {
				t.Fatalf("WriteFile() error = %v", err)
			}
			abortErr := errors.New("cleanup failed")
			store := &removeSourceAndFailAbortStore{ArchiveStore: &localstore.ArchiveStore{}, source: source, abortErr: abortErr}
			runner := newRunner(store, nil, nil, io.Discard, io.Discard)
			result := runner.Run(context.Background(), cli.Options{
				Mode: cli.ModeCreate, Archive: filepath.Join(root, archiveName), Chdir: root, Members: []string{"source.txt"},
			})
			if result.ExitCode != ExitFatal || !errors.Is(result.Err, abortErr) {
				t.Fatalf("Run() = %+v, want joined abort error", result)
			}
		})
	}
}

func TestCreateStdoutPreflightFailureDoesNotOpenWriter(t *testing.T) {
	root := t.TempDir()
	openCalls := 0
	local := fakeLocalArchiveStore{openWriter: func(locator.Ref) (io.WriteCloser, error) {
		openCalls++
		return &fakeWriteCloser{}, nil
	}}
	runner := newRunner(local, nil, nil, io.Discard, io.Discard)
	result := runner.Run(context.Background(), cli.Options{Mode: cli.ModeCreate, Archive: "-", Chdir: root, Members: []string{"missing"}})
	if result.ExitCode != ExitFatal || result.Err == nil {
		t.Fatalf("Run() = %+v, want fatal preflight error", result)
	}
	if openCalls != 0 {
		t.Fatalf("openWriter calls = %d, want 0", openCalls)
	}
}

func TestCreateRejectsUnsafeSymlinkBeforeReplacingArchive(t *testing.T) {
	root := t.TempDir()
	archivePath := filepath.Join(root, "archive.tar")
	original := []byte("original archive")
	if err := os.WriteFile(archivePath, original, 0o644); err != nil {
		t.Fatalf("WriteFile(archive) error = %v", err)
	}
	if err := os.Symlink("/etc/hosts", filepath.Join(root, "unsafe")); err != nil {
		t.Fatalf("Symlink() error = %v", err)
	}

	runner := newRunner(&localstore.ArchiveStore{}, nil, nil, io.Discard, io.Discard)
	result := runner.Run(context.Background(), cli.Options{Mode: cli.ModeCreate, Archive: archivePath, Chdir: root, Members: []string{"unsafe"}})
	if result.ExitCode != ExitFatal || result.Err == nil || !strings.Contains(result.Err.Error(), "absolute target") {
		t.Fatalf("Run() = %+v, want unsafe symlink error", result)
	}
	got, err := os.ReadFile(archivePath)
	if err != nil || !bytes.Equal(got, original) {
		t.Fatalf("archive = %q, err=%v, want original bytes", got, err)
	}
}

func TestNewDoesNotLoadAWSConfigForLocalOperation(t *testing.T) {
	t.Setenv("AWS_PROFILE", "gotgz-profile-that-does-not-exist")
	archivePath := filepath.Join(t.TempDir(), "local.tar")
	if err := os.WriteFile(archivePath, tarArchiveBytes(t, map[string]string{"file.txt": "payload"}), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	var stdout bytes.Buffer
	runner, err := New(context.Background(), &stdout, io.Discard)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	result := runner.Run(context.Background(), cli.Options{Mode: cli.ModeList, Archive: archivePath})
	if result.ExitCode != ExitSuccess || !strings.Contains(stdout.String(), "file.txt") {
		t.Fatalf("Run() = %+v stdout=%q", result, stdout.String())
	}
}
