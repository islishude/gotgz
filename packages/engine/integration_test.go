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

	"github.com/islishude/gotgz/packages/cli"
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

func TestCreateExtractLocalRoundTripUsesArchiveSuffixCompression(t *testing.T) {
	root := t.TempDir()
	src := filepath.Join(root, "src")
	out := filepath.Join(root, "out")
	archive := filepath.Join(root, "a.tar.gz")

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

	create, err := cli.Parse([]string{"-c", "-f", archive, "-C", root, "src"})
	if err != nil {
		t.Fatalf("Parse(create) error = %v", err)
	}
	if create.Compression != cli.CompressionGzip {
		t.Fatalf("compression = %q, want %q", create.Compression, cli.CompressionGzip)
	}
	if got := r.Run(context.Background(), create); got.ExitCode != ExitSuccess {
		t.Fatalf("create exit=%d err=%v", got.ExitCode, got.Err)
	}

	if err := os.MkdirAll(out, 0o755); err != nil {
		t.Fatal(err)
	}
	extract, err := cli.Parse([]string{"-x", "-f", archive, "-C", out})
	if err != nil {
		t.Fatalf("Parse(extract) error = %v", err)
	}
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

func TestCreateExtractLocalZipRoundTripUsesArchiveSuffixFormat(t *testing.T) {
	root := t.TempDir()
	src := filepath.Join(root, "src")
	out := filepath.Join(root, "out")
	archive := filepath.Join(root, "a.zip")

	if err := os.MkdirAll(src, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "hello.txt"), []byte("zip-world"), 0o644); err != nil {
		t.Fatal(err)
	}

	r, err := New(context.Background(), io.Discard, io.Discard)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	create, err := cli.Parse([]string{"-c", "-f", archive, "-C", root, "src"})
	if err != nil {
		t.Fatalf("Parse(create) error = %v", err)
	}
	if create.Compression != cli.CompressionAuto {
		t.Fatalf("compression = %q, want %q", create.Compression, cli.CompressionAuto)
	}
	if got := r.Run(context.Background(), create); got.ExitCode != ExitSuccess {
		t.Fatalf("create exit=%d err=%v", got.ExitCode, got.Err)
	}

	if err := os.MkdirAll(out, 0o755); err != nil {
		t.Fatal(err)
	}
	extract, err := cli.Parse([]string{"-x", "-f", archive, "-C", out})
	if err != nil {
		t.Fatalf("Parse(extract) error = %v", err)
	}
	if got := r.Run(context.Background(), extract); got.ExitCode != ExitSuccess {
		t.Fatalf("extract exit=%d err=%v", got.ExitCode, got.Err)
	}

	b, err := os.ReadFile(filepath.Join(out, filepath.Base(src), "hello.txt"))
	if err != nil {
		t.Fatalf("read extracted file: %v", err)
	}
	if string(b) != "zip-world" {
		t.Fatalf("content mismatch = %q", string(b))
	}
}

func TestCreateExtractLocalSplitRoundTrip(t *testing.T) {
	root := t.TempDir()
	archive := filepath.Join(root, "bundle.tar")
	out := filepath.Join(root, "out")

	if err := os.WriteFile(filepath.Join(root, "one.txt"), []byte("one"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "two.txt"), []byte("two"), 0o644); err != nil {
		t.Fatal(err)
	}

	r, err := New(context.Background(), io.Discard, io.Discard)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	create := cli.Options{
		Mode:           cli.ModeCreate,
		Archive:        archive,
		Chdir:          root,
		SplitSizeBytes: 1,
		Members:        []string{"one.txt", "two.txt"},
	}
	if got := r.Run(context.Background(), create); got.ExitCode != ExitSuccess {
		t.Fatalf("create exit=%d err=%v", got.ExitCode, got.Err)
	}

	firstPart := filepath.Join(root, "bundle.part0001.tar")
	secondPart := filepath.Join(root, "bundle.part0002.tar")
	if _, err := os.Stat(firstPart); err != nil {
		t.Fatalf("expected first split archive: %v", err)
	}
	if _, err := os.Stat(secondPart); err != nil {
		t.Fatalf("expected second split archive: %v", err)
	}
	if _, err := os.Stat(archive); !os.IsNotExist(err) {
		t.Fatalf("base archive should not exist when split mode is enabled, err=%v", err)
	}

	var listBuf bytes.Buffer
	rList, err := New(context.Background(), &listBuf, io.Discard)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	list := cli.Options{Mode: cli.ModeList, Archive: firstPart}
	if got := rList.Run(context.Background(), list); got.ExitCode != ExitSuccess {
		t.Fatalf("list exit=%d err=%v", got.ExitCode, got.Err)
	}
	for _, want := range []string{"one.txt", "two.txt"} {
		if !strings.Contains(listBuf.String(), want) {
			t.Fatalf("split listing missing %q:\n%s", want, listBuf.String())
		}
	}

	if err := os.MkdirAll(out, 0o755); err != nil {
		t.Fatal(err)
	}
	rExtract, err := New(context.Background(), io.Discard, io.Discard)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	extract := cli.Options{Mode: cli.ModeExtract, Archive: firstPart, Chdir: out}
	if got := rExtract.Run(context.Background(), extract); got.ExitCode != ExitSuccess {
		t.Fatalf("extract exit=%d err=%v", got.ExitCode, got.Err)
	}

	for _, tc := range []struct {
		name string
		want string
	}{
		{name: "one.txt", want: "one"},
		{name: "two.txt", want: "two"},
	} {
		b, err := os.ReadFile(filepath.Join(out, tc.name))
		if err != nil {
			t.Fatalf("read %s: %v", tc.name, err)
		}
		if string(b) != tc.want {
			t.Fatalf("%s = %q, want %q", tc.name, string(b), tc.want)
		}
	}
}

func TestCreateLocalSplitSingleVolumeStillUsesPart0001(t *testing.T) {
	root := t.TempDir()
	archive := filepath.Join(root, "bundle.tar.gz")

	if err := os.WriteFile(filepath.Join(root, "one.txt"), []byte("one"), 0o644); err != nil {
		t.Fatal(err)
	}

	r, err := New(context.Background(), io.Discard, io.Discard)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	create := cli.Options{
		Mode:           cli.ModeCreate,
		Archive:        archive,
		Chdir:          root,
		Compression:    cli.CompressionGzip,
		SplitSizeBytes: 1 << 20,
		Members:        []string{"one.txt"},
	}
	if got := r.Run(context.Background(), create); got.ExitCode != ExitSuccess {
		t.Fatalf("create exit=%d err=%v", got.ExitCode, got.Err)
	}

	if _, err := os.Stat(filepath.Join(root, "bundle.part0001.tar.gz")); err != nil {
		t.Fatalf("expected single split archive with part0001 suffix: %v", err)
	}
	if _, err := os.Stat(archive); !os.IsNotExist(err) {
		t.Fatalf("base archive should not exist when split mode is enabled, err=%v", err)
	}
}

func TestListSplitArchiveFailsWhenVolumeMissing(t *testing.T) {
	root := t.TempDir()
	archive := filepath.Join(root, "bundle.tar")

	for _, name := range []string{"one.txt", "two.txt", "three.txt"} {
		if err := os.WriteFile(filepath.Join(root, name), []byte(name), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	r, err := New(context.Background(), io.Discard, io.Discard)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	create := cli.Options{
		Mode:           cli.ModeCreate,
		Archive:        archive,
		Chdir:          root,
		SplitSizeBytes: 1,
		Members:        []string{"one.txt", "two.txt", "three.txt"},
	}
	if got := r.Run(context.Background(), create); got.ExitCode != ExitSuccess {
		t.Fatalf("create exit=%d err=%v", got.ExitCode, got.Err)
	}

	if err := os.Remove(filepath.Join(root, "bundle.part0002.tar")); err != nil {
		t.Fatalf("remove split archive: %v", err)
	}

	rList, err := New(context.Background(), io.Discard, io.Discard)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	list := cli.Options{Mode: cli.ModeList, Archive: filepath.Join(root, "bundle.part0001.tar")}
	got := rList.Run(context.Background(), list)
	if got.ExitCode != ExitFatal {
		t.Fatalf("list exit=%d err=%v, want fatal", got.ExitCode, got.Err)
	}
	if got.Err == nil || !strings.Contains(got.Err.Error(), "missing split archive volume") {
		t.Fatalf("list err=%v, want missing split archive volume", got.Err)
	}
}

func TestSplitArchiveDiscoveryWithDotRelativePath(t *testing.T) {
	root := t.TempDir()
	archive := filepath.Join(root, "bundle.tar")
	out := filepath.Join(root, "out")

	for _, tc := range []struct {
		name string
		body string
	}{
		{name: "one.txt", body: "one"},
		{name: "two.txt", body: "two"},
	} {
		if err := os.WriteFile(filepath.Join(root, tc.name), []byte(tc.body), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	r, err := New(context.Background(), io.Discard, io.Discard)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	create := cli.Options{
		Mode:           cli.ModeCreate,
		Archive:        archive,
		Chdir:          root,
		SplitSizeBytes: 1,
		Members:        []string{"one.txt", "two.txt"},
	}
	if got := r.Run(context.Background(), create); got.ExitCode != ExitSuccess {
		t.Fatalf("create exit=%d err=%v", got.ExitCode, got.Err)
	}

	firstPart := filepath.Join(root, "bundle.part0001.tar")
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd() error = %v", err)
	}
	relFirstPart, err := filepath.Rel(wd, firstPart)
	if err != nil {
		t.Fatalf("Rel() error = %v", err)
	}
	archiveArg := "." + string(filepath.Separator) + relFirstPart

	var listBuf bytes.Buffer
	rList, err := New(context.Background(), &listBuf, io.Discard)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	list := cli.Options{Mode: cli.ModeList, Archive: archiveArg}
	if got := rList.Run(context.Background(), list); got.ExitCode != ExitSuccess {
		t.Fatalf("list exit=%d err=%v", got.ExitCode, got.Err)
	}
	for _, want := range []string{"one.txt", "two.txt"} {
		if !strings.Contains(listBuf.String(), want) {
			t.Fatalf("relative split listing missing %q:\n%s", want, listBuf.String())
		}
	}

	if err := os.MkdirAll(out, 0o755); err != nil {
		t.Fatal(err)
	}
	rExtract, err := New(context.Background(), io.Discard, io.Discard)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	extract := cli.Options{Mode: cli.ModeExtract, Archive: archiveArg, Chdir: out}
	if got := rExtract.Run(context.Background(), extract); got.ExitCode != ExitSuccess {
		t.Fatalf("extract exit=%d err=%v", got.ExitCode, got.Err)
	}
	for _, tc := range []struct {
		name string
		want string
	}{
		{name: "one.txt", want: "one"},
		{name: "two.txt", want: "two"},
	} {
		b, err := os.ReadFile(filepath.Join(out, tc.name))
		if err != nil {
			t.Fatalf("read %s: %v", tc.name, err)
		}
		if string(b) != tc.want {
			t.Fatalf("%s = %q, want %q", tc.name, string(b), tc.want)
		}
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

func TestExtractTarHardLinkEntryCreatesHardLink(t *testing.T) {
	root := t.TempDir()
	archive := filepath.Join(root, "hardlink.tar")
	out := filepath.Join(root, "out")

	payload := tarArchiveBytesWithHardLink(t, "dir/original.txt", "hardlink-content", "dir/alias.txt")
	if err := os.WriteFile(archive, payload, 0o644); err != nil {
		t.Fatalf("write tar: %v", err)
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
	if got.ExitCode != ExitSuccess {
		t.Fatalf("extract exit=%d err=%v", got.ExitCode, got.Err)
	}

	original := filepath.Join(out, "dir", "original.txt")
	alias := filepath.Join(out, "dir", "alias.txt")

	b, err := os.ReadFile(alias)
	if err != nil {
		t.Fatalf("read hardlink file: %v", err)
	}
	if string(b) != "hardlink-content" {
		t.Fatalf("hardlink content = %q, want %q", string(b), "hardlink-content")
	}

	origInfo, err := os.Stat(original)
	if err != nil {
		t.Fatalf("stat original: %v", err)
	}
	aliasInfo, err := os.Stat(alias)
	if err != nil {
		t.Fatalf("stat alias: %v", err)
	}
	if !os.SameFile(origInfo, aliasInfo) {
		t.Fatalf("expected %s to be a hard link to %s", alias, original)
	}
}

func TestExtractTarHardLinkTargetEscapesExtractionDir(t *testing.T) {
	root := t.TempDir()
	archive := filepath.Join(root, "hardlink-escape.tar")
	out := filepath.Join(root, "out")

	payload := tarArchiveHardLinkBytes(t, "dir/alias.txt", "../../../etc/passwd")
	if err := os.WriteFile(archive, payload, 0o644); err != nil {
		t.Fatalf("write tar: %v", err)
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
	if got.Err == nil || !strings.Contains(got.Err.Error(), "outside target directory") {
		t.Fatalf("extract err=%v, want outside target directory error", got.Err)
	}

	if _, err := os.Lstat(filepath.Join(out, "dir", "alias.txt")); !os.IsNotExist(err) {
		t.Fatalf("escaping hard link entry should not create output file, lstat err=%v", err)
	}
}

func TestExtractTarRejectsPreexistingSymlinkTraversal(t *testing.T) {
	cases := []struct {
		name        string
		build       func(t *testing.T) []byte
		blockedPath string
	}{
		{
			name: "regular file",
			build: func(t *testing.T) []byte {
				return tarArchiveBytes(t, map[string]string{"dir/file.txt": "payload"})
			},
			blockedPath: "file.txt",
		},
		{
			name: "directory",
			build: func(t *testing.T) []byte {
				t.Helper()

				var buf bytes.Buffer
				tw := tar.NewWriter(&buf)
				if err := tw.WriteHeader(&tar.Header{
					Name:     "dir/sub",
					Mode:     0o755,
					Typeflag: tar.TypeDir,
					Format:   tar.FormatPAX,
				}); err != nil {
					t.Fatalf("WriteHeader(dir/sub): %v", err)
				}
				if err := tw.Close(); err != nil {
					t.Fatalf("close tar writer: %v", err)
				}
				return buf.Bytes()
			},
			blockedPath: "sub",
		},
		{
			name: "symlink entry",
			build: func(t *testing.T) []byte {
				t.Helper()

				var buf bytes.Buffer
				tw := tar.NewWriter(&buf)
				if err := tw.WriteHeader(&tar.Header{
					Name:     "dir/link",
					Mode:     0o777,
					Typeflag: tar.TypeSymlink,
					Linkname: "safe.txt",
					Format:   tar.FormatPAX,
				}); err != nil {
					t.Fatalf("WriteHeader(dir/link): %v", err)
				}
				if err := tw.Close(); err != nil {
					t.Fatalf("close tar writer: %v", err)
				}
				return buf.Bytes()
			},
			blockedPath: "link",
		},
		{
			name: "hard link entry",
			build: func(t *testing.T) []byte {
				return tarArchiveBytesWithHardLink(t, "src/original.txt", "payload", "dir/alias.txt")
			},
			blockedPath: "alias.txt",
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()
			archive := filepath.Join(root, "preexisting-symlink.tar")
			out := filepath.Join(root, "out")
			outside := filepath.Join(root, "outside")

			if err := os.MkdirAll(out, 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.MkdirAll(outside, 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(outside, filepath.Join(out, "dir")); err != nil {
				t.Fatalf("symlink output dir: %v", err)
			}
			if err := os.WriteFile(archive, tt.build(t), 0o644); err != nil {
				t.Fatalf("write tar: %v", err)
			}

			r, err := New(context.Background(), io.Discard, io.Discard)
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}

			got := r.Run(context.Background(), cli.Options{
				Mode:    cli.ModeExtract,
				Archive: archive,
				Chdir:   out,
			})
			if got.ExitCode != ExitFatal {
				t.Fatalf("extract exit=%d err=%v, want fatal", got.ExitCode, got.Err)
			}
			if got.Err == nil || !strings.Contains(got.Err.Error(), "follow symlink") {
				t.Fatalf("extract err=%v, want symlink traversal error", got.Err)
			}
			if _, err := os.Lstat(filepath.Join(outside, tt.blockedPath)); !os.IsNotExist(err) {
				t.Fatalf("outside path should remain absent, lstat err=%v", err)
			}
		})
	}
}

func TestExtractTarSameOwnerDoesNotFail(t *testing.T) {
	root := t.TempDir()
	archive := filepath.Join(root, "same-owner.tar")
	out := filepath.Join(root, "out")

	payload := tarArchiveBytesWithOwnership(t, "dir/file.txt", "same-owner-content", 424242, 434343)
	if err := os.WriteFile(archive, payload, 0o644); err != nil {
		t.Fatalf("write tar: %v", err)
	}
	if err := os.MkdirAll(out, 0o755); err != nil {
		t.Fatal(err)
	}

	r, err := New(context.Background(), io.Discard, io.Discard)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	sameOwner := true
	extract := cli.Options{
		Mode:      cli.ModeExtract,
		Archive:   archive,
		Chdir:     out,
		SameOwner: &sameOwner,
	}
	got := r.Run(context.Background(), extract)
	if got.ExitCode != ExitSuccess {
		t.Fatalf("extract exit=%d err=%v", got.ExitCode, got.Err)
	}

	b, err := os.ReadFile(filepath.Join(out, "dir", "file.txt"))
	if err != nil {
		t.Fatalf("read extracted file: %v", err)
	}
	if string(b) != "same-owner-content" {
		t.Fatalf("content mismatch = %q", string(b))
	}
}

func TestExtractTarDefaultTypeSkipsEntryAndContinues(t *testing.T) {
	root := t.TempDir()
	archive := filepath.Join(root, "default-type.tar")
	out := filepath.Join(root, "out")

	payload := tarArchiveBytesWithDefaultTypeEntry(t, "meta/ignored.pax", "ignored-payload", "dir/after.txt", "after-content")
	if err := os.WriteFile(archive, payload, 0o644); err != nil {
		t.Fatalf("write tar: %v", err)
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
	if got.ExitCode != ExitSuccess {
		t.Fatalf("extract exit=%d err=%v", got.ExitCode, got.Err)
	}

	if _, err := os.Lstat(filepath.Join(out, "meta", "ignored.pax")); !os.IsNotExist(err) {
		t.Fatalf("default type entry should be skipped, lstat err=%v", err)
	}
	b, err := os.ReadFile(filepath.Join(out, "dir", "after.txt"))
	if err != nil {
		t.Fatalf("read subsequent file: %v", err)
	}
	if string(b) != "after-content" {
		t.Fatalf("subsequent content mismatch = %q", string(b))
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

// tarArchiveBytesWithHardLink builds an in-memory tar archive containing one
// regular file and one hard-link entry pointing at it.
func tarArchiveBytesWithHardLink(t *testing.T, targetName, targetContent, linkName string) []byte {
	t.Helper()

	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)

	targetHdr := &tar.Header{
		Name:     targetName,
		Mode:     0o644,
		Size:     int64(len(targetContent)),
		Typeflag: tar.TypeReg,
		Format:   tar.FormatPAX,
	}
	if err := tw.WriteHeader(targetHdr); err != nil {
		t.Fatalf("WriteHeader(%q): %v", targetName, err)
	}
	if _, err := io.WriteString(tw, targetContent); err != nil {
		t.Fatalf("Write(%q): %v", targetName, err)
	}

	if err := tw.WriteHeader(&tar.Header{
		Name:     linkName,
		Mode:     0o644,
		Typeflag: tar.TypeLink,
		Linkname: targetName,
		Format:   tar.FormatPAX,
	}); err != nil {
		t.Fatalf("WriteHeader(%q): %v", linkName, err)
	}

	if err := tw.Close(); err != nil {
		t.Fatalf("close tar writer: %v", err)
	}
	return buf.Bytes()
}

// tarArchiveHardLinkBytes builds an in-memory tar archive with one hard-link
// entry that links to linkTarget.
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

// tarArchiveBytesWithOwnership builds an in-memory tar archive with one regular
// file carrying explicit uid/gid ownership metadata.
func tarArchiveBytesWithOwnership(t *testing.T, name, content string, uid, gid int) []byte {
	t.Helper()

	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	if err := tw.WriteHeader(&tar.Header{
		Name:     name,
		Mode:     0o644,
		Size:     int64(len(content)),
		Typeflag: tar.TypeReg,
		Uid:      uid,
		Gid:      gid,
		Format:   tar.FormatPAX,
	}); err != nil {
		t.Fatalf("WriteHeader(%q): %v", name, err)
	}
	if _, err := io.WriteString(tw, content); err != nil {
		t.Fatalf("Write(%q): %v", name, err)
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("close tar writer: %v", err)
	}
	return buf.Bytes()
}

// tarArchiveBytesWithDefaultTypeEntry builds an archive with one entry that
// falls into extractToLocal's default switch branch, followed by a regular file.
func tarArchiveBytesWithDefaultTypeEntry(t *testing.T, skippedName, skippedPayload, nextName, nextContent string) []byte {
	t.Helper()

	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)

	if err := tw.WriteHeader(&tar.Header{
		Name:     skippedName,
		Mode:     0o644,
		Size:     int64(len(skippedPayload)),
		Typeflag: byte('V'),
		Format:   tar.FormatPAX,
	}); err != nil {
		t.Fatalf("WriteHeader(%q): %v", skippedName, err)
	}
	if _, err := io.WriteString(tw, skippedPayload); err != nil {
		t.Fatalf("Write(%q): %v", skippedName, err)
	}

	if err := tw.WriteHeader(&tar.Header{
		Name:     nextName,
		Mode:     0o644,
		Size:     int64(len(nextContent)),
		Typeflag: tar.TypeReg,
		Format:   tar.FormatPAX,
	}); err != nil {
		t.Fatalf("WriteHeader(%q): %v", nextName, err)
	}
	if _, err := io.WriteString(tw, nextContent); err != nil {
		t.Fatalf("Write(%q): %v", nextName, err)
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

func TestCreateExtractLocalZipSymlinkRoundTrip(t *testing.T) {
	root := t.TempDir()
	src := filepath.Join(root, "src")
	out := filepath.Join(root, "out")
	archive := filepath.Join(root, "a.zip")

	if err := os.MkdirAll(src, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "target.txt"), []byte("link-target"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("target.txt", filepath.Join(src, "link.txt")); err != nil {
		t.Skipf("symlink is not supported on this environment: %v", err)
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

	linkPath := filepath.Join(out, "src", "link.txt")
	info, err := os.Lstat(linkPath)
	if err != nil {
		t.Fatalf("lstat symlink: %v", err)
	}
	if info.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("%s is not a symlink, mode=%v", linkPath, info.Mode())
	}
	target, err := os.Readlink(linkPath)
	if err != nil {
		t.Fatalf("readlink: %v", err)
	}
	if target != "target.txt" {
		t.Fatalf("symlink target = %q, want %q", target, "target.txt")
	}

	b, err := os.ReadFile(linkPath)
	if err != nil {
		t.Fatalf("read symlinked file: %v", err)
	}
	if string(b) != "link-target" {
		t.Fatalf("symlinked content mismatch = %q", string(b))
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

func TestExtractZipSymlinkTargetEscapesExtractionDir(t *testing.T) {
	root := t.TempDir()
	archive := filepath.Join(root, "symlink-escape.zip")
	out := filepath.Join(root, "out")

	payload := zipArchiveSymlinkBytes(t, "link", "../../../etc/passwd")
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
	if got.Err == nil || !strings.Contains(got.Err.Error(), "escapes extraction directory") {
		t.Fatalf("extract err=%v, want escapes extraction directory error", got.Err)
	}

	if _, err := os.Lstat(filepath.Join(out, "link")); !os.IsNotExist(err) {
		t.Fatalf("escaping symlink entry should not create output file, lstat err=%v", err)
	}
}

func TestExtractZipRejectsPreexistingSymlinkTraversal(t *testing.T) {
	cases := []struct {
		name        string
		build       func(t *testing.T) []byte
		blockedPath string
	}{
		{
			name: "regular file",
			build: func(t *testing.T) []byte {
				return zipArchiveBytes(t, map[string]string{"dir/file.txt": "payload"})
			},
			blockedPath: "file.txt",
		},
		{
			name: "directory",
			build: func(t *testing.T) []byte {
				t.Helper()

				var buf bytes.Buffer
				zw := zip.NewWriter(&buf)
				hdr := &zip.FileHeader{Name: "dir/sub/", Method: zip.Store}
				hdr.SetMode(fs.ModeDir | 0o755)
				if _, err := zw.CreateHeader(hdr); err != nil {
					t.Fatalf("CreateHeader(dir/sub/): %v", err)
				}
				if err := zw.Close(); err != nil {
					t.Fatalf("close zip writer: %v", err)
				}
				return buf.Bytes()
			},
			blockedPath: "sub",
		},
		{
			name: "symlink entry",
			build: func(t *testing.T) []byte {
				return zipArchiveSymlinkBytes(t, "dir/link", "safe.txt")
			},
			blockedPath: "link",
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()
			archive := filepath.Join(root, "preexisting-symlink.zip")
			out := filepath.Join(root, "out")
			outside := filepath.Join(root, "outside")

			if err := os.MkdirAll(out, 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.MkdirAll(outside, 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(outside, filepath.Join(out, "dir")); err != nil {
				t.Fatalf("symlink output dir: %v", err)
			}
			if err := os.WriteFile(archive, tt.build(t), 0o644); err != nil {
				t.Fatalf("write zip: %v", err)
			}

			r, err := New(context.Background(), io.Discard, io.Discard)
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}

			got := r.Run(context.Background(), cli.Options{
				Mode:    cli.ModeExtract,
				Archive: archive,
				Chdir:   out,
			})
			if got.ExitCode != ExitFatal {
				t.Fatalf("extract exit=%d err=%v, want fatal", got.ExitCode, got.Err)
			}
			if got.Err == nil || !strings.Contains(got.Err.Error(), "follow symlink") {
				t.Fatalf("extract err=%v, want symlink traversal error", got.Err)
			}
			if _, err := os.Lstat(filepath.Join(outside, tt.blockedPath)); !os.IsNotExist(err) {
				t.Fatalf("outside path should remain absent, lstat err=%v", err)
			}
		})
	}
}
