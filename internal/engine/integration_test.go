package engine

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/islishude/gotgz/internal/cli"
)

func TestCreateExtractLocalRoundTrip(t *testing.T) {
	root := t.TempDir()
	src := filepath.Join(root, "src")
	out := filepath.Join(root, "out")
	archive := filepath.Join(root, "a.tar")

	if err := os.MkdirAll(src, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "hello.txt"), []byte("world"), 0o644); err != nil {
		t.Fatal(err)
	}

	r, err := New(context.Background(), io.Discard, io.Discard)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	create := cli.Options{Mode: cli.ModeCreate, Archive: archive, Chdir: root, Members: []string{"src"}}
	if got := r.Run(context.Background(), create); got.ExitCode != ExitSuccess {
		t.Fatalf("create exit=%d err=%v", got.ExitCode, got.Err)
	}

	if err := os.MkdirAll(out, 0o755); err != nil {
		t.Fatal(err)
	}
	extract := cli.Options{Mode: cli.ModeExtract, Archive: archive, Chdir: out}
	if got := r.Run(context.Background(), extract); got.ExitCode != ExitSuccess {
		t.Fatalf("extract exit=%d err=%v", got.ExitCode, got.Err)
	}

	b, err := os.ReadFile(filepath.Join(out, filepath.Base(src), "hello.txt"))
	if err != nil {
		t.Fatalf("read extracted file: %v", err)
	}
	if string(b) != "world" {
		t.Fatalf("content mismatch = %q", string(b))
	}
}

func TestExtractStripComponents(t *testing.T) {
	root := t.TempDir()
	srcRoot := filepath.Join(root, "src")
	archive := filepath.Join(root, "a.tar")
	out := filepath.Join(root, "out")

	if err := os.MkdirAll(filepath.Join(srcRoot, "dir"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(srcRoot, "dir", "file.txt"), []byte("strip-me"), 0o644); err != nil {
		t.Fatal(err)
	}

	r, err := New(context.Background(), io.Discard, io.Discard)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	create := cli.Options{Mode: cli.ModeCreate, Archive: archive, Chdir: root, Members: []string{"src"}}
	if got := r.Run(context.Background(), create); got.ExitCode != ExitSuccess {
		t.Fatalf("create exit=%d err=%v", got.ExitCode, got.Err)
	}

	if err := os.MkdirAll(out, 0o755); err != nil {
		t.Fatal(err)
	}
	extract := cli.Options{Mode: cli.ModeExtract, Archive: archive, Chdir: out, StripComponents: 1}
	if got := r.Run(context.Background(), extract); got.ExitCode != ExitSuccess {
		t.Fatalf("extract exit=%d err=%v", got.ExitCode, got.Err)
	}

	data, err := os.ReadFile(filepath.Join(out, "dir", "file.txt"))
	if err != nil {
		t.Fatalf("read stripped file: %v", err)
	}
	if string(data) != "strip-me" {
		t.Fatalf("content mismatch = %q", string(data))
	}
	if _, err := os.Stat(filepath.Join(out, "src")); !os.IsNotExist(err) {
		t.Fatalf("src directory should not exist after strip, err=%v", err)
	}
}

func TestCreateArchiveWithSuffix(t *testing.T) {
	root := t.TempDir()
	src := filepath.Join(root, "src")
	out := filepath.Join(root, "out")
	archiveBase := filepath.Join(root, "backup.tar.gz")
	archiveWithSuffix := AddArchiveSuffix(archiveBase, "custom")

	if err := os.MkdirAll(src, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "hello.txt"), []byte("world"), 0o644); err != nil {
		t.Fatal(err)
	}

	r, err := New(context.Background(), io.Discard, io.Discard)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	create := cli.Options{
		Mode:        cli.ModeCreate,
		Archive:     archiveBase,
		Suffix:      "custom",
		Compression: cli.CompressionGzip,
		Chdir:       root,
		Members:     []string{"src"},
	}
	if got := r.Run(context.Background(), create); got.ExitCode != ExitSuccess {
		t.Fatalf("create exit=%d err=%v", got.ExitCode, got.Err)
	}

	if _, err := os.Stat(archiveWithSuffix); err != nil {
		t.Fatalf("expected suffixed archive %s: %v", archiveWithSuffix, err)
	}
	if _, err := os.Stat(archiveBase); !os.IsNotExist(err) {
		t.Fatalf("base archive should not exist when suffix is set, err=%v", err)
	}

	if err := os.MkdirAll(out, 0o755); err != nil {
		t.Fatal(err)
	}
	extract := cli.Options{Mode: cli.ModeExtract, Archive: archiveWithSuffix, Chdir: out}
	if got := r.Run(context.Background(), extract); got.ExitCode != ExitSuccess {
		t.Fatalf("extract exit=%d err=%v", got.ExitCode, got.Err)
	}

	b, err := os.ReadFile(filepath.Join(out, "src", "hello.txt"))
	if err != nil {
		t.Fatalf("read extracted file: %v", err)
	}
	if string(b) != "world" {
		t.Fatalf("content mismatch = %q", string(b))
	}
}

// testdataDir returns the absolute path to the top-level testdata directory.
func testdataDir(t *testing.T) string {
	t.Helper()
	// integration_test.go lives in internal/engine, testdata is at repo root.
	p, err := filepath.Abs(filepath.Join("..", "..", "testdata"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(p); err != nil {
		t.Fatalf("testdata directory not found at %s: %v", p, err)
	}
	return p
}

// TestTestdataRoundTrip compresses the testdata/parent directory (excluding
// .exclude), extracts it into a temporary directory, and verifies that every
// file's content, permissions, and symlinks are preserved.
func TestTestdataRoundTrip(t *testing.T) {
	srcRoot := testdataDir(t)
	srcDir := filepath.Join(srcRoot, "parent")

	root := t.TempDir()
	archivePath := filepath.Join(root, "testdata.tar.gz")
	outDir := filepath.Join(root, "out")

	r, err := New(context.Background(), io.Discard, io.Discard)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	samePerms := true

	// --- Create archive ---
	create := cli.Options{
		Mode:        cli.ModeCreate,
		Archive:     archivePath,
		Chdir:       srcRoot,
		Compression: cli.CompressionGzip,
		Exclude:     []string{"parent/.exclude", "parent/.exclude/*"},
		Members:     []string{"parent"},
	}
	if got := r.Run(context.Background(), create); got.ExitCode != ExitSuccess {
		t.Fatalf("create exit=%d err=%v", got.ExitCode, got.Err)
	}

	// --- Extract archive ---
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		t.Fatal(err)
	}
	extract := cli.Options{
		Mode:            cli.ModeExtract,
		Archive:         archivePath,
		Chdir:           outDir,
		Compression:     cli.CompressionGzip,
		SamePermissions: &samePerms,
	}
	if got := r.Run(context.Background(), extract); got.ExitCode != ExitSuccess {
		t.Fatalf("extract exit=%d err=%v", got.ExitCode, got.Err)
	}

	extractedParent := filepath.Join(outDir, "parent")

	// --- Verify .exclude was excluded ---
	excludeDir := filepath.Join(extractedParent, ".exclude")
	if _, err := os.Stat(excludeDir); !os.IsNotExist(err) {
		t.Fatalf(".exclude directory should not exist in extraction, but got err=%v", err)
	}

	// --- Walk the source and compare ---
	err = filepath.WalkDir(srcDir, func(srcPath string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}

		rel, err := filepath.Rel(srcDir, srcPath)
		if err != nil {
			return err
		}

		// Skip .exclude subtree – it was excluded from the archive.
		if rel == ".exclude" || strings.HasPrefix(rel, ".exclude"+string(filepath.Separator)) {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}

		dstPath := filepath.Join(extractedParent, rel)

		srcInfo, err := os.Lstat(srcPath)
		if err != nil {
			t.Fatalf("lstat source %s: %v", srcPath, err)
		}
		dstInfo, err := os.Lstat(dstPath)
		if err != nil {
			t.Fatalf("lstat extracted %s: %v (source=%s)", dstPath, err, rel)
		}

		// Type must match.
		if srcInfo.Mode().Type() != dstInfo.Mode().Type() {
			t.Errorf("%s: type mismatch src=%s dst=%s", rel, srcInfo.Mode().Type(), dstInfo.Mode().Type())
			return nil
		}

		// Check symlinks.
		if srcInfo.Mode()&os.ModeSymlink != 0 {
			srcTarget, err := os.Readlink(srcPath)
			if err != nil {
				t.Fatalf("%s: readlink source: %v", rel, err)
			}
			dstTarget, err := os.Readlink(dstPath)
			if err != nil {
				t.Fatalf("%s: readlink extracted: %v", rel, err)
			}
			if srcTarget != dstTarget {
				t.Errorf("%s: symlink target mismatch src=%q dst=%q", rel, srcTarget, dstTarget)
			}
			return nil
		}

		// Check permissions for non-symlinks.
		if srcInfo.Mode().Perm() != dstInfo.Mode().Perm() {
			t.Errorf("%s: permission mismatch src=%o dst=%o", rel, srcInfo.Mode().Perm(), dstInfo.Mode().Perm())
		}

		// Check file content.
		if srcInfo.Mode().IsRegular() {
			srcData, err := os.ReadFile(srcPath)
			if err != nil {
				t.Fatalf("%s: read source: %v", rel, err)
			}
			dstData, err := os.ReadFile(dstPath)
			if err != nil {
				t.Fatalf("%s: read extracted: %v", rel, err)
			}
			if !bytes.Equal(srcData, dstData) {
				t.Errorf("%s: content mismatch (src %d bytes, dst %d bytes)", rel, len(srcData), len(dstData))
			}
		}

		return nil
	})
	if err != nil {
		t.Fatalf("walk source: %v", err)
	}

	// --- Verify no extra files in extracted tree ---
	err = filepath.WalkDir(extractedParent, func(dstPath string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(extractedParent, dstPath)
		if err != nil {
			return err
		}
		srcPath := filepath.Join(srcDir, rel)
		if _, err := os.Lstat(srcPath); os.IsNotExist(err) {
			t.Errorf("extra file in extraction: %s", rel)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk extracted: %v", err)
	}
}

// TestTestdataRoundTripCompressors runs the same round-trip with every
// supported compression algorithm to ensure they all preserve data faithfully.
func TestTestdataRoundTripCompressors(t *testing.T) {
	compressors := []cli.CompressionHint{
		cli.CompressionNone,
		cli.CompressionGzip,
		cli.CompressionZstd,
		cli.CompressionLz4,
	}

	srcRoot := testdataDir(t)
	samePerms := true

	for _, comp := range compressors {
		t.Run(string(comp), func(t *testing.T) {
			root := t.TempDir()
			archivePath := filepath.Join(root, "testdata.tar")
			outDir := filepath.Join(root, "out")

			r, err := New(context.Background(), io.Discard, io.Discard)
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}

			create := cli.Options{
				Mode:        cli.ModeCreate,
				Archive:     archivePath,
				Chdir:       srcRoot,
				Compression: comp,
				Exclude:     []string{"parent/.exclude", "parent/.exclude/*"},
				Members:     []string{"parent"},
			}
			if got := r.Run(context.Background(), create); got.ExitCode != ExitSuccess {
				t.Fatalf("create exit=%d err=%v", got.ExitCode, got.Err)
			}

			if err := os.MkdirAll(outDir, 0o755); err != nil {
				t.Fatal(err)
			}
			extract := cli.Options{
				Mode:            cli.ModeExtract,
				Archive:         archivePath,
				Chdir:           outDir,
				Compression:     comp,
				SamePermissions: &samePerms,
			}
			if got := r.Run(context.Background(), extract); got.ExitCode != ExitSuccess {
				t.Fatalf("extract exit=%d err=%v", got.ExitCode, got.Err)
			}

			extractedParent := filepath.Join(outDir, "parent")

			// .exclude must not exist
			if _, err := os.Stat(filepath.Join(extractedParent, ".exclude")); !os.IsNotExist(err) {
				t.Fatalf(".exclude should not exist, err=%v", err)
			}

			// Spot-check regular files by reading source content directly
			srcDir := filepath.Join(srcRoot, "parent")
			for _, rel := range []string{"README.md", "index.json"} {
				want, err := os.ReadFile(filepath.Join(srcDir, rel))
				if err != nil {
					t.Fatalf("%s: read source: %v", rel, err)
				}
				got, err := os.ReadFile(filepath.Join(extractedParent, rel))
				if err != nil {
					t.Fatalf("%s: read extracted: %v", rel, err)
				}
				if !bytes.Equal(want, got) {
					t.Errorf("%s: content mismatch (src %d bytes, dst %d bytes)", rel, len(want), len(got))
				}
			}

			// Spot-check symlinks
			for _, sym := range []string{"javascript", "README"} {
				srcTarget, err := os.Readlink(filepath.Join(srcDir, sym))
				if err != nil {
					t.Fatalf("readlink source %s: %v", sym, err)
				}
				dstTarget, err := os.Readlink(filepath.Join(extractedParent, sym))
				if err != nil {
					t.Fatalf("readlink extracted %s: %v", sym, err)
				}
				if srcTarget != dstTarget {
					t.Errorf("%s: symlink target mismatch src=%q dst=%q", sym, srcTarget, dstTarget)
				}
			}
		})
	}
}

// tarArchiveBytes builds an in-memory tar archive from name->content pairs.
func tarArchiveBytes(t *testing.T, files map[string]string) []byte {
	t.Helper()

	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for name, content := range files {
		hdr := &tar.Header{
			Name:     name,
			Mode:     0o644,
			Size:     int64(len(content)),
			Typeflag: tar.TypeReg,
			Format:   tar.FormatPAX,
		}
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatalf("WriteHeader(%q): %v", name, err)
		}
		if _, err := io.WriteString(tw, content); err != nil {
			t.Fatalf("Write(%q): %v", name, err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("close tar writer: %v", err)
	}
	return buf.Bytes()
}

// zipArchiveBytes builds an in-memory zip archive from name->content pairs.
func zipArchiveBytes(t *testing.T, files map[string]string) []byte {
	t.Helper()

	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	for name, content := range files {
		w, err := zw.Create(name)
		if err != nil {
			t.Fatalf("Create(%q): %v", name, err)
		}
		if _, err := io.WriteString(w, content); err != nil {
			t.Fatalf("Write(%q): %v", name, err)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("close zip writer: %v", err)
	}
	return buf.Bytes()
}

// zipArchiveSymlinkBytes builds an in-memory zip archive with one symlink entry.
func zipArchiveSymlinkBytes(t *testing.T, name, target string) []byte {
	t.Helper()

	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	hdr := &zip.FileHeader{
		Name:   name,
		Method: zip.Store,
	}
	hdr.SetMode(os.ModeSymlink | 0o777)
	w, err := zw.CreateHeader(hdr)
	if err != nil {
		t.Fatalf("CreateHeader(%q): %v", name, err)
	}
	if _, err := io.WriteString(w, target); err != nil {
		t.Fatalf("Write(%q): %v", name, err)
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("close zip writer: %v", err)
	}
	return buf.Bytes()
}

// zipArchiveBytesUnsupportedMethod builds a zip with one entry using an
// unsupported compression method code.
func zipArchiveBytesUnsupportedMethod(t *testing.T, name string, method uint16) []byte {
	t.Helper()

	var buf bytes.Buffer
	write := func(v any) {
		if err := binary.Write(&buf, binary.LittleEndian, v); err != nil {
			t.Fatalf("binary write: %v", err)
		}
	}

	fileName := []byte(name)

	// Local file header.
	write(uint32(0x04034b50))
	write(uint16(20))
	write(uint16(0))
	write(method)
	write(uint16(0))
	write(uint16(0))
	write(uint32(0))
	write(uint32(0))
	write(uint32(0))
	write(uint16(len(fileName)))
	write(uint16(0))
	if _, err := buf.Write(fileName); err != nil {
		t.Fatalf("write local filename: %v", err)
	}

	cdOffset := buf.Len()

	// Central directory header.
	write(uint32(0x02014b50))
	write(uint16(20))
	write(uint16(20))
	write(uint16(0))
	write(method)
	write(uint16(0))
	write(uint16(0))
	write(uint32(0))
	write(uint32(0))
	write(uint32(0))
	write(uint16(len(fileName)))
	write(uint16(0))
	write(uint16(0))
	write(uint16(0))
	write(uint16(0))
	write(uint32(0))
	write(uint32(0))
	if _, err := buf.Write(fileName); err != nil {
		t.Fatalf("write central filename: %v", err)
	}

	cdSize := buf.Len() - cdOffset

	// End of central directory.
	write(uint32(0x06054b50))
	write(uint16(0))
	write(uint16(0))
	write(uint16(1))
	write(uint16(1))
	write(uint32(cdSize))
	write(uint32(cdOffset))
	write(uint16(0))

	return buf.Bytes()
}

func TestListHTTPArchive(t *testing.T) {
	archiveBytes := tarArchiveBytes(t, map[string]string{
		"files/one.txt": "one",
		"files/two.txt": "two",
	})

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(archiveBytes)
	}))
	defer server.Close()

	var stdout bytes.Buffer
	r, err := New(context.Background(), &stdout, io.Discard)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	list := cli.Options{Mode: cli.ModeList, Archive: server.URL + "/archive.tar"}
	if got := r.Run(context.Background(), list); got.ExitCode != ExitSuccess {
		t.Fatalf("list exit=%d err=%v", got.ExitCode, got.Err)
	}

	output := stdout.String()
	for _, item := range []string{"files/one.txt", "files/two.txt"} {
		if !strings.Contains(output, item) {
			t.Fatalf("list output missing %q:\n%s", item, output)
		}
	}
}

func TestExtractHTTPArchive(t *testing.T) {
	archiveBytes := tarArchiveBytes(t, map[string]string{
		"dir/hello.txt": "hello-http",
	})

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(archiveBytes)
	}))
	defer server.Close()

	outDir := t.TempDir()
	r, err := New(context.Background(), io.Discard, io.Discard)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	extract := cli.Options{
		Mode:    cli.ModeExtract,
		Archive: server.URL + "/archive.tar",
		Chdir:   outDir,
	}
	if got := r.Run(context.Background(), extract); got.ExitCode != ExitSuccess {
		t.Fatalf("extract exit=%d err=%v", got.ExitCode, got.Err)
	}

	b, err := os.ReadFile(filepath.Join(outDir, "dir", "hello.txt"))
	if err != nil {
		t.Fatalf("read extracted file: %v", err)
	}
	if string(b) != "hello-http" {
		t.Fatalf("content mismatch = %q", string(b))
	}
}

func TestCreateToHTTPArchiveTargetFails(t *testing.T) {
	root := t.TempDir()
	src := filepath.Join(root, "src")
	if err := os.MkdirAll(src, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "a.txt"), []byte("a"), 0o644); err != nil {
		t.Fatal(err)
	}

	r, err := New(context.Background(), io.Discard, io.Discard)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	create := cli.Options{
		Mode:    cli.ModeCreate,
		Archive: "https://example.com/archive.tar",
		Chdir:   root,
		Members: []string{"src"},
	}
	got := r.Run(context.Background(), create)
	if got.ExitCode != ExitFatal {
		t.Fatalf("exit=%d, want %d", got.ExitCode, ExitFatal)
	}
	if got.Err == nil || !strings.Contains(got.Err.Error(), "unsupported archive target") {
		t.Fatalf("err = %v", got.Err)
	}
}

func TestExtractTarRespectsContextCancellation(t *testing.T) {
	root := t.TempDir()
	src := filepath.Join(root, "src")
	archive := filepath.Join(root, "a.tar")
	out := filepath.Join(root, "out")

	if err := os.MkdirAll(src, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "hello.txt"), []byte("world"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(out, 0o755); err != nil {
		t.Fatal(err)
	}

	r, err := New(context.Background(), io.Discard, io.Discard)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	create := cli.Options{Mode: cli.ModeCreate, Archive: archive, Chdir: root, Members: []string{"src"}}
	if got := r.Run(context.Background(), create); got.ExitCode != ExitSuccess {
		t.Fatalf("create exit=%d err=%v", got.ExitCode, got.Err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	extract := cli.Options{Mode: cli.ModeExtract, Archive: archive, Chdir: out}
	got := r.Run(ctx, extract)
	if got.ExitCode != ExitFatal {
		t.Fatalf("extract exit=%d err=%v, want fatal", got.ExitCode, got.Err)
	}
	if !errors.Is(got.Err, context.Canceled) {
		t.Fatalf("extract err=%v, want context canceled", got.Err)
	}
}

func TestCreateExtractLocalZipRoundTrip(t *testing.T) {
	root := t.TempDir()
	src := filepath.Join(root, "src")
	out := filepath.Join(root, "out")
	archive := filepath.Join(root, "a.zip")

	if err := os.MkdirAll(src, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "hello.txt"), []byte("world"), 0o644); err != nil {
		t.Fatal(err)
	}

	r, err := New(context.Background(), io.Discard, io.Discard)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	create := cli.Options{Mode: cli.ModeCreate, Archive: archive, Chdir: root, Members: []string{"src"}}
	if got := r.Run(context.Background(), create); got.ExitCode != ExitSuccess {
		t.Fatalf("create exit=%d err=%v", got.ExitCode, got.Err)
	}

	if err := os.MkdirAll(out, 0o755); err != nil {
		t.Fatal(err)
	}
	extract := cli.Options{Mode: cli.ModeExtract, Archive: archive, Chdir: out}
	if got := r.Run(context.Background(), extract); got.ExitCode != ExitSuccess {
		t.Fatalf("extract exit=%d err=%v", got.ExitCode, got.Err)
	}

	b, err := os.ReadFile(filepath.Join(out, "src", "hello.txt"))
	if err != nil {
		t.Fatalf("read extracted file: %v", err)
	}
	if string(b) != "world" {
		t.Fatalf("content mismatch = %q", string(b))
	}
}

func TestExtractZipStripComponents(t *testing.T) {
	root := t.TempDir()
	srcRoot := filepath.Join(root, "src")
	archive := filepath.Join(root, "a.zip")
	out := filepath.Join(root, "out")

	if err := os.MkdirAll(filepath.Join(srcRoot, "dir"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(srcRoot, "dir", "file.txt"), []byte("strip-me"), 0o644); err != nil {
		t.Fatal(err)
	}

	r, err := New(context.Background(), io.Discard, io.Discard)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	create := cli.Options{Mode: cli.ModeCreate, Archive: archive, Chdir: root, Members: []string{"src"}}
	if got := r.Run(context.Background(), create); got.ExitCode != ExitSuccess {
		t.Fatalf("create exit=%d err=%v", got.ExitCode, got.Err)
	}

	if err := os.MkdirAll(out, 0o755); err != nil {
		t.Fatal(err)
	}
	extract := cli.Options{Mode: cli.ModeExtract, Archive: archive, Chdir: out, StripComponents: 1}
	if got := r.Run(context.Background(), extract); got.ExitCode != ExitSuccess {
		t.Fatalf("extract exit=%d err=%v", got.ExitCode, got.Err)
	}

	data, err := os.ReadFile(filepath.Join(out, "dir", "file.txt"))
	if err != nil {
		t.Fatalf("read stripped file: %v", err)
	}
	if string(data) != "strip-me" {
		t.Fatalf("content mismatch = %q", string(data))
	}
}

func TestExtractZipToStdout(t *testing.T) {
	root := t.TempDir()
	src := filepath.Join(root, "src")
	archive := filepath.Join(root, "a.zip")

	if err := os.MkdirAll(src, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "hello.txt"), []byte("zip-stdout"), 0o644); err != nil {
		t.Fatal(err)
	}

	var out bytes.Buffer
	r, err := New(context.Background(), &out, io.Discard)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	create := cli.Options{Mode: cli.ModeCreate, Archive: archive, Chdir: root, Members: []string{"src"}}
	if got := r.Run(context.Background(), create); got.ExitCode != ExitSuccess {
		t.Fatalf("create exit=%d err=%v", got.ExitCode, got.Err)
	}

	extract := cli.Options{
		Mode:      cli.ModeExtract,
		Archive:   archive,
		ToStdout:  true,
		Wildcards: true,
		Members:   []string{"*/hello.txt"},
	}
	if got := r.Run(context.Background(), extract); got.ExitCode != ExitSuccess {
		t.Fatalf("extract exit=%d err=%v", got.ExitCode, got.Err)
	}
	if out.String() != "zip-stdout" {
		t.Fatalf("stdout content mismatch = %q", out.String())
	}
}

func TestListHTTPZipArchive(t *testing.T) {
	archiveBytes := zipArchiveBytes(t, map[string]string{
		"files/one.txt": "one",
		"files/two.txt": "two",
	})

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			t.Fatalf("response writer does not implement http.Flusher")
		}
		mid := len(archiveBytes) / 2
		_, _ = w.Write(archiveBytes[:mid])
		flusher.Flush()
		_, _ = w.Write(archiveBytes[mid:])
	}))
	defer server.Close()

	var stdout bytes.Buffer
	r, err := New(context.Background(), &stdout, io.Discard)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	list := cli.Options{Mode: cli.ModeList, Archive: server.URL + "/archive.tar"}
	if got := r.Run(context.Background(), list); got.ExitCode != ExitSuccess {
		t.Fatalf("list exit=%d err=%v", got.ExitCode, got.Err)
	}

	output := stdout.String()
	for _, item := range []string{"files/one.txt", "files/two.txt"} {
		if !strings.Contains(output, item) {
			t.Fatalf("list output missing %q:\n%s", item, output)
		}
	}
}

func TestExtractHTTPZipArchive(t *testing.T) {
	archiveBytes := zipArchiveBytes(t, map[string]string{
		"dir/hello.txt": "hello-zip-http",
	})

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			t.Fatalf("response writer does not implement http.Flusher")
		}
		mid := len(archiveBytes) / 2
		_, _ = w.Write(archiveBytes[:mid])
		flusher.Flush()
		_, _ = w.Write(archiveBytes[mid:])
	}))
	defer server.Close()

	outDir := t.TempDir()
	r, err := New(context.Background(), io.Discard, io.Discard)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	extract := cli.Options{
		Mode:    cli.ModeExtract,
		Archive: server.URL + "/archive.tar",
		Chdir:   outDir,
	}
	if got := r.Run(context.Background(), extract); got.ExitCode != ExitSuccess {
		t.Fatalf("extract exit=%d err=%v", got.ExitCode, got.Err)
	}

	b, err := os.ReadFile(filepath.Join(outDir, "dir", "hello.txt"))
	if err != nil {
		t.Fatalf("read extracted file: %v", err)
	}
	if string(b) != "hello-zip-http" {
		t.Fatalf("content mismatch = %q", string(b))
	}
}

func TestZipCreateIgnoredOptionsReturnWarning(t *testing.T) {
	root := t.TempDir()
	src := filepath.Join(root, "src")
	archive := filepath.Join(root, "a.zip")

	if err := os.MkdirAll(src, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "hello.txt"), []byte("world"), 0o644); err != nil {
		t.Fatal(err)
	}

	r, err := New(context.Background(), io.Discard, io.Discard)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	create := cli.Options{
		Mode:        cli.ModeCreate,
		Archive:     archive,
		Compression: cli.CompressionGzip,
		ACL:         true,
		Xattrs:      true,
		Chdir:       root,
		Members:     []string{"src"},
	}
	got := r.Run(context.Background(), create)
	if got.ExitCode != ExitWarning {
		t.Fatalf("create exit=%d err=%v, want warning", got.ExitCode, got.Err)
	}
}

func TestZipExtractIgnoredOptionsReturnWarning(t *testing.T) {
	root := t.TempDir()
	src := filepath.Join(root, "src")
	archive := filepath.Join(root, "a.zip")
	out := filepath.Join(root, "out")

	if err := os.MkdirAll(src, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "hello.txt"), []byte("world"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(out, 0o755); err != nil {
		t.Fatal(err)
	}

	r, err := New(context.Background(), io.Discard, io.Discard)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	create := cli.Options{Mode: cli.ModeCreate, Archive: archive, Chdir: root, Members: []string{"src"}}
	if got := r.Run(context.Background(), create); got.ExitCode != ExitSuccess {
		t.Fatalf("create exit=%d err=%v", got.ExitCode, got.Err)
	}

	sameOwner := true
	extract := cli.Options{
		Mode:         cli.ModeExtract,
		Archive:      archive,
		Chdir:        out,
		Compression:  cli.CompressionGzip,
		ACL:          true,
		Xattrs:       true,
		NumericOwner: true,
		SameOwner:    &sameOwner,
	}
	got := r.Run(context.Background(), extract)
	if got.ExitCode != ExitWarning {
		t.Fatalf("extract exit=%d err=%v, want warning", got.ExitCode, got.Err)
	}
}

func TestExtractZipUnsupportedEntrySkipsWithoutEmptyFile(t *testing.T) {
	root := t.TempDir()
	archive := filepath.Join(root, "unsupported.zip")
	out := filepath.Join(root, "out")

	payload := zipArchiveBytesUnsupportedMethod(t, "dir/bad.txt", 99)
	if err := os.WriteFile(archive, payload, 0o644); err != nil {
		t.Fatalf("write unsupported zip: %v", err)
	}
	if err := os.MkdirAll(out, 0o755); err != nil {
		t.Fatal(err)
	}

	r, err := New(context.Background(), io.Discard, io.Discard)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	extract := cli.Options{
		Mode:    cli.ModeExtract,
		Archive: archive,
		Chdir:   out,
	}
	got := r.Run(context.Background(), extract)
	if got.ExitCode != ExitWarning {
		t.Fatalf("extract exit=%d err=%v, want warning", got.ExitCode, got.Err)
	}

	if _, err := os.Stat(filepath.Join(out, "dir", "bad.txt")); !os.IsNotExist(err) {
		t.Fatalf("unsupported zip entry should not create output file, stat err=%v", err)
	}
}

func TestListZipUnsupportedEntryDoesNotWarn(t *testing.T) {
	root := t.TempDir()
	archive := filepath.Join(root, "unsupported.zip")

	payload := zipArchiveBytesUnsupportedMethod(t, "dir/bad.txt", 99)
	if err := os.WriteFile(archive, payload, 0o644); err != nil {
		t.Fatalf("write unsupported zip: %v", err)
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	r, err := New(context.Background(), &stdout, &stderr)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	list := cli.Options{
		Mode:    cli.ModeList,
		Archive: archive,
	}
	got := r.Run(context.Background(), list)
	if got.ExitCode != ExitSuccess {
		t.Fatalf("list exit=%d err=%v, want success", got.ExitCode, got.Err)
	}
	if !strings.Contains(stdout.String(), "dir/bad.txt") {
		t.Fatalf("list output = %q, want listed unsupported entry", stdout.String())
	}
	if strings.Contains(stderr.String(), "unsupported algorithm") {
		t.Fatalf("list stderr should not report unsupported algorithm warnings, got %q", stderr.String())
	}
}

func TestExtractZipSymlinkTargetTooLarge(t *testing.T) {
	root := t.TempDir()
	archive := filepath.Join(root, "symlink-too-large.zip")
	out := filepath.Join(root, "out")

	target := strings.Repeat("a", maxZipSymlinkTargetBytes+1)
	payload := zipArchiveSymlinkBytes(t, "link", target)
	if err := os.WriteFile(archive, payload, 0o644); err != nil {
		t.Fatalf("write zip: %v", err)
	}
	if err := os.MkdirAll(out, 0o755); err != nil {
		t.Fatal(err)
	}

	r, err := New(context.Background(), io.Discard, io.Discard)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	extract := cli.Options{
		Mode:    cli.ModeExtract,
		Archive: archive,
		Chdir:   out,
	}
	got := r.Run(context.Background(), extract)
	if got.ExitCode != ExitFatal {
		t.Fatalf("extract exit=%d err=%v, want fatal", got.ExitCode, got.Err)
	}
	if got.Err == nil || !strings.Contains(got.Err.Error(), "target exceeds") {
		t.Fatalf("extract err=%v, want target exceeds error", got.Err)
	}

	if _, err := os.Lstat(filepath.Join(out, "link")); !os.IsNotExist(err) {
		t.Fatalf("oversized symlink entry should not create output file, lstat err=%v", err)
	}
}
