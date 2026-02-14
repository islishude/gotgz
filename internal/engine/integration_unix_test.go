//go:build unix

package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"testing"

	"github.com/islishude/gotgz/internal/cli"
)

func TestCreateExtractFromTestdata_ExcludeAndPreserveMetadata(t *testing.T) {
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
