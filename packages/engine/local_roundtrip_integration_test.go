//go:build integration

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
)

func TestIntegrationLocalTarRoundTrip(t *testing.T) {
	tests := []struct {
		name    string
		archive string
	}{
		{name: "plain tar", archive: "bundle.tar"},
		{name: "gzip tar", archive: "bundle.tar.gz"},
		{name: "zstd tar", archive: "bundle.tar.zst"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()
			writeFixtureTree(t, root, []fixtureEntry{{path: "src/docs/readme.txt", body: "readme"}, {path: "src/assets/app.js", body: "console.log('ok')"}, {path: "src/latest", symlink: "docs/readme.txt"}})
			archivePath := filepath.Join(root, tt.archive)
			outDir := filepath.Join(root, "out")

			r, err := New(context.Background(), io.Discard, io.Discard)
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}
			if got := r.Run(context.Background(), cli.Options{Mode: cli.ModeCreate, Archive: archivePath, Chdir: root, Members: []string{"src"}, Compression: compressionForArchive(tt.archive)}); got.ExitCode != ExitSuccess {
				t.Fatalf("create exit=%d err=%v", got.ExitCode, got.Err)
			}
			if err := os.MkdirAll(outDir, 0o755); err != nil {
				t.Fatalf("MkdirAll() error = %v", err)
			}
			if got := r.Run(context.Background(), cli.Options{Mode: cli.ModeExtract, Archive: archivePath, Chdir: outDir}); got.ExitCode != ExitSuccess {
				t.Fatalf("extract exit=%d err=%v", got.ExitCode, got.Err)
			}
			if got := mustReadFile(t, filepath.Join(outDir, "src", "docs", "readme.txt")); got != "readme" {
				t.Fatalf("readme.txt = %q, want readme", got)
			}
			linkTarget, err := os.Readlink(filepath.Join(outDir, "src", "latest"))
			if err != nil {
				t.Fatalf("Readlink() error = %v", err)
			}
			if linkTarget != "docs/readme.txt" {
				t.Fatalf("link target = %q, want docs/readme.txt", linkTarget)
			}
		})
	}
}

func TestIntegrationLocalZipRoundTrip(t *testing.T) {
	root := t.TempDir()
	writeFixtureTree(t, root, []fixtureEntry{{path: "src/hello.txt", body: "zip-world"}, {path: "src/nested/value.txt", body: "nested"}})
	archivePath := filepath.Join(root, "bundle.zip")
	outDir := filepath.Join(root, "out")

	r, err := New(context.Background(), io.Discard, io.Discard)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if got := r.Run(context.Background(), cli.Options{Mode: cli.ModeCreate, Archive: archivePath, Chdir: root, Members: []string{"src"}}); got.ExitCode != ExitSuccess {
		t.Fatalf("create exit=%d err=%v", got.ExitCode, got.Err)
	}
	if got := r.Run(context.Background(), cli.Options{Mode: cli.ModeExtract, Archive: archivePath, Chdir: outDir}); got.ExitCode != ExitSuccess {
		t.Fatalf("extract exit=%d err=%v", got.ExitCode, got.Err)
	}
	if got := mustReadFile(t, filepath.Join(outDir, "src", "hello.txt")); got != "zip-world" {
		t.Fatalf("hello.txt = %q, want zip-world", got)
	}
}

func TestIntegrationSplitArchiveReopen(t *testing.T) {
	tests := []struct {
		name    string
		archive string
	}{
		{name: "split tar", archive: "bundle.tar.gz"},
		{name: "split zip", archive: "bundle.zip"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()
			writeFixtureTree(t, root, []fixtureEntry{{path: "one.txt", body: "one"}, {path: "two.txt", body: "two"}})
			archivePath := filepath.Join(root, tt.archive)
			outDir := filepath.Join(root, "out")

			r, err := New(context.Background(), io.Discard, io.Discard)
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}
			create := cli.Options{Mode: cli.ModeCreate, Archive: archivePath, Chdir: root, Members: []string{"one.txt", "two.txt"}, SplitSizeBytes: 1, Compression: compressionForArchive(tt.archive)}
			if got := r.Run(context.Background(), create); got.ExitCode != ExitSuccess {
				t.Fatalf("create exit=%d err=%v", got.ExitCode, got.Err)
			}

			firstPart := strings.Replace(archivePath, filepath.Ext(archivePath), ".part0001"+filepath.Ext(archivePath), 1)
			if strings.HasSuffix(tt.archive, ".tar.gz") {
				firstPart = filepath.Join(root, "bundle.part0001.tar.gz")
			}
			if _, err := os.Stat(firstPart); err != nil {
				t.Fatalf("Stat(%s) error = %v", firstPart, err)
			}

			var stdout bytes.Buffer
			listRunner, err := New(context.Background(), &stdout, io.Discard)
			if err != nil {
				t.Fatalf("New() list error = %v", err)
			}
			if got := listRunner.Run(context.Background(), cli.Options{Mode: cli.ModeList, Archive: firstPart}); got.ExitCode != ExitSuccess {
				t.Fatalf("list exit=%d err=%v", got.ExitCode, got.Err)
			}
			if !strings.Contains(stdout.String(), "one.txt") || !strings.Contains(stdout.String(), "two.txt") {
				t.Fatalf("list output = %q", stdout.String())
			}

			if got := r.Run(context.Background(), cli.Options{Mode: cli.ModeExtract, Archive: firstPart, Chdir: outDir}); got.ExitCode != ExitSuccess {
				t.Fatalf("extract exit=%d err=%v", got.ExitCode, got.Err)
			}
			if mustReadFile(t, filepath.Join(outDir, "one.txt")) != "one" || mustReadFile(t, filepath.Join(outDir, "two.txt")) != "two" {
				t.Fatalf("split extract did not restore both files")
			}
		})
	}
}

func TestIntegrationSplitTarHardlinkFallsBackToSerial(t *testing.T) {
	root := t.TempDir()
	firstPart := filepath.Join(root, "bundle.part0001.tar")
	secondPart := filepath.Join(root, "bundle.part0002.tar")
	outDir := filepath.Join(root, "out")
	if err := os.WriteFile(firstPart, tarArchiveBytes(t, map[string]string{"dir/target.txt": "payload"}), 0o644); err != nil {
		t.Fatalf("WriteFile(firstPart) error = %v", err)
	}
	if err := os.WriteFile(secondPart, tarArchiveHardLinkBytes(t, "dir/alias.txt", "dir/target.txt"), 0o644); err != nil {
		t.Fatalf("WriteFile(secondPart) error = %v", err)
	}

	r := newLocalSplitExtractTestRunner()
	var plan splitExtractPlan
	r.splitExtractHooks = &splitExtractHooks{onPlan: func(got splitExtractPlan) { plan = got }}
	if got := r.Run(context.Background(), cli.Options{Mode: cli.ModeExtract, Archive: firstPart, Chdir: outDir}); got.ExitCode != ExitSuccess {
		t.Fatalf("extract exit=%d err=%v", got.ExitCode, got.Err)
	}
	if plan.parallel {
		t.Fatal("plan.parallel = true, want false")
	}
	if plan.serialReason != splitExtractSerialReasonLocalHardlink {
		t.Fatalf("serialReason = %q, want %q", plan.serialReason, splitExtractSerialReasonLocalHardlink)
	}
	targetInfo, err := os.Stat(filepath.Join(outDir, "dir", "target.txt"))
	if err != nil {
		t.Fatalf("Stat(target) error = %v", err)
	}
	aliasInfo, err := os.Stat(filepath.Join(outDir, "dir", "alias.txt"))
	if err != nil {
		t.Fatalf("Stat(alias) error = %v", err)
	}
	if !os.SameFile(targetInfo, aliasInfo) {
		t.Fatal("target.txt and alias.txt should be hardlinked")
	}
}

func mustReadFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%s) error = %v", path, err)
	}
	return string(b)
}

func tarArchiveHardLinkBytes(t *testing.T, name, linkTarget string) []byte {
	t.Helper()

	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	if err := tw.WriteHeader(&tar.Header{
		Name:     name,
		Mode:     0o644,
		Typeflag: tar.TypeLink,
		Linkname: linkTarget,
		Format:   tar.FormatPAX,
	}); err != nil {
		t.Fatalf("WriteHeader(%q): %v", name, err)
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("close tar writer: %v", err)
	}
	return buf.Bytes()
}
