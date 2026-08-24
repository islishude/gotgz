package engine

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/islishude/gotgz/packages/cli"
	localstore "github.com/islishude/gotgz/packages/storage/local"
)

func TestExtractDirectoryMetadataDoesNotFollowReplacementSymlink(t *testing.T) {
	tests := []struct {
		name    string
		suffix  string
		archive func(*testing.T) []byte
	}{
		{name: "tar", suffix: ".tar", archive: directoryThenSymlinkTar},
		{name: "zip", suffix: ".zip", archive: directoryThenSymlinkZip},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()
			outDir := filepath.Join(root, "out")
			if err := os.Mkdir(outDir, 0o755); err != nil {
				t.Fatalf("Mkdir(out) error = %v", err)
			}
			t.Cleanup(func() { _ = os.Chmod(outDir, 0o700) })
			before, err := os.Stat(outDir)
			if err != nil {
				t.Fatalf("Stat(out before) error = %v", err)
			}
			archivePath := filepath.Join(root, "replacement"+tt.suffix)
			if err := os.WriteFile(archivePath, tt.archive(t), 0o600); err != nil {
				t.Fatalf("WriteFile(archive) error = %v", err)
			}

			runner := newRunner(&localstore.ArchiveStore{}, nil, nil, io.Discard, io.Discard)
			result := runner.Run(context.Background(), cli.Options{
				Mode: cli.ModeExtract, Archive: archivePath, Chdir: outDir, Members: []string{"victim"},
			})
			if result.ExitCode != ExitSuccess {
				t.Fatalf("Run() = %+v", result)
			}
			after, err := os.Stat(outDir)
			if err != nil {
				t.Fatalf("Stat(out after) error = %v", err)
			}
			if after.Mode().Perm() != before.Mode().Perm() {
				t.Fatalf("output mode = %o, want unchanged %o", after.Mode().Perm(), before.Mode().Perm())
			}
			if target, err := os.Readlink(filepath.Join(outDir, "victim")); err != nil || target != "." {
				t.Fatalf("Readlink(victim) = %q, %v, want .", target, err)
			}
		})
	}
}

func TestLocalMetadataSessionRejectsDirectoryTypeDrift(t *testing.T) {
	root := t.TempDir()
	t.Cleanup(func() { _ = os.Chmod(root, 0o700) })
	before, err := os.Stat(root)
	if err != nil {
		t.Fatalf("Stat(root before) error = %v", err)
	}
	target := filepath.Join(root, "victim")
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatalf("Mkdir(victim) error = %v", err)
	}
	session := newLocalMetadataSession(&Runner{stderr: io.Discard}, nil)
	session.queueDirectory(localMetadataRecord{target: target, archiveName: "victim", perm: 0})
	if err := os.Remove(target); err != nil {
		t.Fatalf("Remove(victim) error = %v", err)
	}
	if err := os.Symlink(".", target); err != nil {
		t.Fatalf("Symlink(victim) error = %v", err)
	}
	if _, err := session.finish(); err == nil {
		t.Fatal("finish() error = nil, want target type drift error")
	}
	after, err := os.Stat(root)
	if err != nil {
		t.Fatalf("Stat(root after) error = %v", err)
	}
	if after.Mode().Perm() != before.Mode().Perm() {
		t.Fatalf("root mode = %o, want unchanged %o", after.Mode().Perm(), before.Mode().Perm())
	}
}

func directoryThenSymlinkTar(t *testing.T) []byte {
	t.Helper()
	var payload bytes.Buffer
	tw := tar.NewWriter(&payload)
	if err := tw.WriteHeader(&tar.Header{Name: "victim", Typeflag: tar.TypeDir, Mode: 0, Format: tar.FormatPAX}); err != nil {
		t.Fatalf("WriteHeader(dir) error = %v", err)
	}
	if err := tw.WriteHeader(&tar.Header{Name: "victim", Typeflag: tar.TypeSymlink, Linkname: ".", Mode: 0o777, Format: tar.FormatPAX}); err != nil {
		t.Fatalf("WriteHeader(symlink) error = %v", err)
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("Close(tar) error = %v", err)
	}
	return payload.Bytes()
}

func directoryThenSymlinkZip(t *testing.T) []byte {
	t.Helper()
	var payload bytes.Buffer
	zw := zip.NewWriter(&payload)
	dir := &zip.FileHeader{Name: "victim/", Method: zip.Store}
	dir.SetMode(os.ModeDir)
	if _, err := zw.CreateHeader(dir); err != nil {
		t.Fatalf("CreateHeader(dir) error = %v", err)
	}
	link := &zip.FileHeader{Name: "victim", Method: zip.Store}
	link.SetMode(os.ModeSymlink | 0o777)
	w, err := zw.CreateHeader(link)
	if err != nil {
		t.Fatalf("CreateHeader(symlink) error = %v", err)
	}
	if _, err := io.WriteString(w, "."); err != nil {
		t.Fatalf("Write(symlink) error = %v", err)
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("Close(zip) error = %v", err)
	}
	return payload.Bytes()
}
