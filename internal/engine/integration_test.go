package engine

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"syscall"
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
	archiveWithSuffix := AddTarSuffix(archiveBase, "custom")

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

func TestCreateExtractFromTestdata_ExcludeAndPreserveMetadata(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("skipping test that relies on Unix permissions and ownership")
	}

	ctx := context.Background()
	fixtureRoot := filepath.Clean(filepath.Join("..", "..", "testdata", "parent"))
	if _, err := os.Stat(fixtureRoot); err != nil {
		t.Fatalf("stat fixture root: %v", err)
	}

	root := t.TempDir()
	src := filepath.Join(root, "parent")
	if err := copyTree(fixtureRoot, src); err != nil {
		t.Fatalf("copy fixture tree: %v", err)
	}

	// Ensure permission checks are meaningful.
	if err := os.Chmod(filepath.Join(src, "js"), 0o750); err != nil {
		t.Fatalf("chmod js dir: %v", err)
	}
	if err := os.Chmod(filepath.Join(src, "js", "index.js"), 0o640); err != nil {
		t.Fatalf("chmod js file: %v", err)
	}
	if err := os.Chmod(filepath.Join(src, "README.md"), 0o755); err != nil {
		t.Fatalf("chmod README.md: %v", err)
	}

	excludeDir := filepath.Join(src, ".exclude")
	if err := os.MkdirAll(excludeDir, 0o700); err != nil {
		t.Fatalf("mkdir .exclude: %v", err)
	}
	if err := os.WriteFile(filepath.Join(excludeDir, "secret.txt"), []byte("must-be-excluded"), 0o600); err != nil {
		t.Fatalf("write excluded file: %v", err)
	}

	r, err := New(ctx, io.Discard, io.Discard)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	archive := filepath.Join(root, "parent.tar.gz")
	create := cli.Options{
		Mode:        cli.ModeCreate,
		Archive:     archive,
		Compression: cli.CompressionGzip,
		Chdir:       root,
		Members:     []string{"parent"},
		Exclude:     []string{"parent/.exclude"},
	}
	if got := r.Run(ctx, create); got.ExitCode != ExitSuccess {
		t.Fatalf("create exit=%d err=%v", got.ExitCode, got.Err)
	}

	out := filepath.Join(root, "out")
	if err := os.MkdirAll(out, 0o755); err != nil {
		t.Fatalf("mkdir out: %v", err)
	}
	samePerms := true
	extract := cli.Options{
		Mode:            cli.ModeExtract,
		Archive:         archive,
		Chdir:           out,
		SamePermissions: &samePerms,
	}
	if got := r.Run(ctx, extract); got.ExitCode != ExitSuccess {
		t.Fatalf("extract exit=%d err=%v", got.ExitCode, got.Err)
	}

	extracted := filepath.Join(out, "parent")
	if _, err := os.Stat(filepath.Join(extracted, ".exclude")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected excluded directory to be absent, stat err=%v", err)
	}

	want, err := snapshotTree(src, ".exclude")
	if err != nil {
		t.Fatalf("snapshot source: %v", err)
	}
	got, err := snapshotTree(extracted, ".exclude")
	if err != nil {
		t.Fatalf("snapshot extracted: %v", err)
	}

	if diff := diffSnapshots(want, got); diff != "" {
		t.Fatalf("round-trip mismatch:\n%s", diff)
	}
}

type fileEntry struct {
	Kind       string
	Perm       fs.FileMode
	UID        uint32
	GID        uint32
	Size       int64
	Hash       string
	LinkTarget string
}

func snapshotTree(root string, ignoreTop string) (map[string]fileEntry, error) {
	out := make(map[string]fileEntry)
	err := filepath.WalkDir(root, func(current string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(root, current)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		if shouldIgnore(rel, ignoreTop) {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}

		info, err := os.Lstat(current)
		if err != nil {
			return err
		}
		st, ok := info.Sys().(*syscall.Stat_t)
		if !ok {
			return errors.New("unsupported stat payload")
		}

		entry := fileEntry{
			Perm: info.Mode().Perm(),
			UID:  st.Uid,
			GID:  st.Gid,
			Size: info.Size(),
		}

		switch {
		case info.Mode()&os.ModeSymlink != 0:
			entry.Kind = "symlink"
			linkTarget, err := os.Readlink(current)
			if err != nil {
				return err
			}
			entry.LinkTarget = linkTarget
		case info.Mode().IsRegular():
			entry.Kind = "file"
			hash, err := fileHash(current)
			if err != nil {
				return err
			}
			entry.Hash = hash
		case info.IsDir():
			entry.Kind = "dir"
		default:
			entry.Kind = "other"
		}

		out[filepath.ToSlash(rel)] = entry
		return nil
	})
	return out, err
}

func shouldIgnore(rel string, top string) bool {
	top = strings.TrimSpace(top)
	if top == "" {
		return false
	}
	rel = filepath.Clean(rel)
	if rel == top {
		return true
	}
	return strings.HasPrefix(rel, top+string(filepath.Separator))
}

func diffSnapshots(want map[string]fileEntry, got map[string]fileEntry) string {
	var b strings.Builder

	allKeys := make(map[string]struct{}, len(want)+len(got))
	for k := range want {
		allKeys[k] = struct{}{}
	}
	for k := range got {
		allKeys[k] = struct{}{}
	}

	keys := make([]string, 0, len(allKeys))
	for k := range allKeys {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		w, wOK := want[k]
		g, gOK := got[k]
		switch {
		case !wOK:
			b.WriteString("unexpected path: " + k + "\n")
		case !gOK:
			b.WriteString("missing path: " + k + "\n")
		default:
			if w.Kind != g.Kind {
				b.WriteString("kind mismatch " + k + "\n")
			}
			if w.Perm != g.Perm {
				b.WriteString("perm mismatch " + k + "\n")
			}
			if w.UID != g.UID || w.GID != g.GID {
				b.WriteString("owner mismatch " + k + "\n")
			}
			if w.Size != g.Size {
				b.WriteString("size mismatch " + k + "\n")
			}
			if w.Hash != g.Hash {
				b.WriteString("content hash mismatch " + k + "\n")
			}
			if w.LinkTarget != g.LinkTarget {
				b.WriteString("symlink target mismatch " + k + "\n")
			}
		}
	}

	return strings.TrimSpace(b.String())
}

func fileHash(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close() //nolint:errcheck

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func copyTree(src, dst string) error {
	return filepath.WalkDir(src, func(current string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(src, current)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)

		info, err := os.Lstat(current)
		if err != nil {
			return err
		}
		switch {
		case info.IsDir():
			return os.MkdirAll(target, info.Mode().Perm())
		case info.Mode()&os.ModeSymlink != 0:
			linkTarget, err := os.Readlink(current)
			if err != nil {
				return err
			}
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return err
			}
			return os.Symlink(linkTarget, target)
		default:
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return err
			}
			data, err := os.ReadFile(current)
			if err != nil {
				return err
			}
			return os.WriteFile(target, data, info.Mode().Perm())
		}
	})
}
