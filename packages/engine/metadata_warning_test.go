package engine

import (
	"archive/tar"
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/islishude/gotgz/packages/cli"
	httpstore "github.com/islishude/gotgz/packages/storage/http"
	localstore "github.com/islishude/gotgz/packages/storage/local"
)

func TestExtractWarnsOnMalformedMetadataAndContinues(t *testing.T) {
	root := t.TempDir()
	archivePath := filepath.Join(root, "metadata.tar")
	outDir := filepath.Join(root, "out")

	var payload bytes.Buffer
	tw := tar.NewWriter(&payload)
	hdr := &tar.Header{
		Name:       "dir/file.txt",
		Mode:       0o644,
		Size:       int64(len("payload")),
		Typeflag:   tar.TypeReg,
		Format:     tar.FormatPAX,
		PAXRecords: map[string]string{"GOTGZ.xattr.bad": "***"},
	}
	if err := tw.WriteHeader(hdr); err != nil {
		t.Fatalf("WriteHeader() error = %v", err)
	}
	if _, err := io.WriteString(tw, "payload"); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := os.WriteFile(archivePath, payload.Bytes(), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}

	var stderr bytes.Buffer
	r := newRunner(&localstore.ArchiveStore{}, nil, httpstore.New(), io.Discard, &stderr)
	got := r.Run(context.Background(), cli.Options{Mode: cli.ModeExtract, Archive: archivePath, Chdir: outDir, Xattrs: true})
	if got.ExitCode != ExitWarning {
		t.Fatalf("ExitCode = %d, want %d (err=%v)", got.ExitCode, ExitWarning, got.Err)
	}
	b, err := os.ReadFile(filepath.Join(outDir, "dir", "file.txt"))
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if string(b) != "payload" {
		t.Fatalf("file.txt = %q, want payload", string(b))
	}
	if !strings.Contains(stderr.String(), "metadata for dir/file.txt is malformed") {
		t.Fatalf("stderr = %q, want metadata warning", stderr.String())
	}
}
