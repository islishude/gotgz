package engine

import (
	"archive/tar"
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/islishude/gotgz/packages/cli"
	localstore "github.com/islishude/gotgz/packages/storage/local"
)

func TestTarExtractRestoresExistingFileModeAndDirectoryMetadataLast(t *testing.T) {
	root := t.TempDir()
	archivePath := filepath.Join(root, "metadata.tar")
	outDir := filepath.Join(root, "out")
	dirPath := filepath.Join(outDir, "dir")
	filePath := filepath.Join(dirPath, "file.txt")
	t.Cleanup(func() { _ = os.Chmod(dirPath, 0o700) })
	if err := os.MkdirAll(dirPath, 0o755); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}
	if err := os.WriteFile(filePath, []byte("old"), 0o777); err != nil {
		t.Fatalf("WriteFile(existing) error = %v", err)
	}

	modTime := time.Unix(946656000, 0).UTC()
	var payload bytes.Buffer
	tw := tar.NewWriter(&payload)
	if err := tw.WriteHeader(&tar.Header{Name: "dir", Typeflag: tar.TypeDir, Mode: 0o500, ModTime: modTime, Format: tar.FormatPAX}); err != nil {
		t.Fatalf("WriteHeader(dir) error = %v", err)
	}
	body := "replacement"
	if err := tw.WriteHeader(&tar.Header{Name: "dir/file.txt", Typeflag: tar.TypeReg, Mode: 0o600, Size: int64(len(body)), Format: tar.FormatPAX}); err != nil {
		t.Fatalf("WriteHeader(file) error = %v", err)
	}
	if _, err := io.WriteString(tw, body); err != nil {
		t.Fatalf("Write(file) error = %v", err)
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("Close(tar) error = %v", err)
	}
	if err := os.WriteFile(archivePath, payload.Bytes(), 0o644); err != nil {
		t.Fatalf("WriteFile(archive) error = %v", err)
	}

	noSamePerms := false
	runner := newRunner(&localstore.ArchiveStore{}, nil, nil, io.Discard, io.Discard)
	result := runner.Run(context.Background(), cli.Options{
		Mode: cli.ModeExtract, Archive: archivePath, Chdir: outDir, SamePermissions: &noSamePerms,
	})
	if result.ExitCode != ExitSuccess {
		t.Fatalf("Run() = %+v", result)
	}
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		t.Fatalf("Stat(file) error = %v", err)
	}
	if got := fileInfo.Mode().Perm(); got != 0o600 {
		t.Fatalf("file mode = %o, want 600", got)
	}
	if got, err := os.ReadFile(filePath); err != nil || string(got) != body {
		t.Fatalf("file = %q, err=%v", got, err)
	}
	dirInfo, err := os.Stat(dirPath)
	if err != nil {
		t.Fatalf("Stat(dir) error = %v", err)
	}
	if got := dirInfo.Mode().Perm(); got != 0o500 {
		t.Fatalf("directory mode = %o, want 500", got)
	}
	if !dirInfo.ModTime().Equal(modTime) {
		t.Fatalf("directory mtime = %v, want %v", dirInfo.ModTime(), modTime)
	}
}

func TestTarExtractStripComponentsTransformsHardlinkTarget(t *testing.T) {
	root := t.TempDir()
	archivePath := filepath.Join(root, "hardlink.tar")
	outDir := filepath.Join(root, "out")
	var payload bytes.Buffer
	tw := tar.NewWriter(&payload)
	body := "payload"
	if err := tw.WriteHeader(&tar.Header{Name: "top/alias", Typeflag: tar.TypeReg, Mode: 0o644, Size: int64(len(body)), Format: tar.FormatPAX}); err != nil {
		t.Fatalf("WriteHeader(alias) error = %v", err)
	}
	if _, err := io.WriteString(tw, body); err != nil {
		t.Fatalf("Write(alias) error = %v", err)
	}
	if err := tw.WriteHeader(&tar.Header{Name: "top/target", Typeflag: tar.TypeLink, Linkname: "top/alias", Mode: 0o644, Format: tar.FormatPAX}); err != nil {
		t.Fatalf("WriteHeader(target) error = %v", err)
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("Close(tar) error = %v", err)
	}
	if err := os.WriteFile(archivePath, payload.Bytes(), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	runner := newRunner(&localstore.ArchiveStore{}, nil, nil, io.Discard, io.Discard)
	result := runner.Run(context.Background(), cli.Options{Mode: cli.ModeExtract, Archive: archivePath, Chdir: outDir, StripComponents: 1})
	if result.ExitCode != ExitSuccess {
		t.Fatalf("Run() = %+v", result)
	}
	aliasInfo, err := os.Stat(filepath.Join(outDir, "alias"))
	if err != nil {
		t.Fatalf("Stat(alias) error = %v", err)
	}
	targetInfo, err := os.Stat(filepath.Join(outDir, "target"))
	if err != nil {
		t.Fatalf("Stat(target) error = %v", err)
	}
	if !os.SameFile(aliasInfo, targetInfo) {
		t.Fatal("stripped hardlink does not reference stripped target")
	}
}
