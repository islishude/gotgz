package engine

import (
	"bytes"
	"context"
	"io"
	"io/fs"
	"os"
	"path/filepath"
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
		if rel == ".exclude" || filepath.HasPrefix(rel, ".exclude"+string(filepath.Separator)) {
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
			if got := r.Run(context.Background(), extract); got.ExitCode == ExitFatal {
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
