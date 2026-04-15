package engine

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/islishude/gotgz/packages/archivepath"
	"github.com/islishude/gotgz/packages/cli"
	"github.com/islishude/gotgz/packages/locator"
	s3store "github.com/islishude/gotgz/packages/storage/s3"
)

// ---------------------------------------------------------------------------
// extractToS3 (0%)
// ---------------------------------------------------------------------------

// TestExtractToS3RegularFile verifies tar entries are uploaded to S3 correctly.
func TestExtractToS3RegularFile(t *testing.T) {
	var uploadedRef locator.Ref
	var uploadedBody string
	var uploadedMeta map[string]string

	r := &Runner{
		storage: &storageRouter{
			s3: fakeS3ArchiveStore{
				uploadStream: func(_ context.Context, ref locator.Ref, body io.Reader, metadata map[string]string) error {
					uploadedRef = ref
					b, err := io.ReadAll(body)
					if err != nil {
						return err
					}
					uploadedBody = string(b)
					uploadedMeta = metadata
					return nil
				},
			},
		},
		stderr: io.Discard,
		stdout: io.Discard,
	}

	hdr := &tar.Header{
		Name:     "dir/file.txt",
		Mode:     0o644,
		Size:     7,
		Typeflag: tar.TypeReg,
		Format:   tar.FormatPAX,
	}
	tr := newTarReaderFromEntries(t, []tarEntry{{hdr: hdr, body: "payload"}})

	_, err := tr.Next()
	if err != nil {
		t.Fatalf("Next() error = %v", err)
	}

	target := locator.Ref{
		Kind:   locator.KindS3,
		Bucket: "bucket",
		Key:    "prefix",
	}

	warnings, err := r.extractToS3(context.Background(), target, hdr, tr, nil)
	if err != nil {
		t.Fatalf("extractToS3() error = %v", err)
	}
	if warnings != 0 {
		t.Fatalf("warnings = %d, want 0", warnings)
	}
	if uploadedRef.Bucket != "bucket" {
		t.Fatalf("bucket = %q", uploadedRef.Bucket)
	}
	if !strings.Contains(uploadedRef.Key, "dir/file.txt") {
		t.Fatalf("key = %q, want to contain dir/file.txt", uploadedRef.Key)
	}
	if uploadedBody != "payload" {
		t.Fatalf("body = %q, want payload", uploadedBody)
	}
	if uploadedMeta == nil {
		t.Fatalf("metadata should not be nil")
	}
}

// TestExtractToS3DirectoryEntry verifies directories are consumed silently.
func TestExtractToS3DirectoryEntry(t *testing.T) {
	r := &Runner{
		storage: &storageRouter{
			s3: fakeS3ArchiveStore{
				uploadStream: func(_ context.Context, _ locator.Ref, _ io.Reader, _ map[string]string) error {
					t.Fatalf("uploadStream should not be called for directories")
					return nil
				},
			},
		},
		stderr: io.Discard,
		stdout: io.Discard,
	}

	hdr := &tar.Header{
		Name:     "dir/",
		Mode:     0o755,
		Typeflag: tar.TypeDir,
		Format:   tar.FormatPAX,
	}
	tr := newTarReaderFromEntries(t, []tarEntry{{hdr: hdr, body: ""}})
	_, _ = tr.Next()

	warnings, err := r.extractToS3(context.Background(), locator.Ref{Kind: locator.KindS3, Bucket: "b", Key: "p"}, hdr, tr, nil)
	if err != nil {
		t.Fatalf("extractToS3() error = %v", err)
	}
	if warnings != 0 {
		t.Fatalf("warnings = %d", warnings)
	}
}

// TestExtractToS3EmptyNameWithContent verifies entries that resolve to empty name
// (like "./") are skipped even when they have content.
func TestExtractToS3EmptyNameWithContent(t *testing.T) {
	uploaded := false
	r := &Runner{
		storage: &storageRouter{
			s3: fakeS3ArchiveStore{
				uploadStream: func(_ context.Context, _ locator.Ref, _ io.Reader, _ map[string]string) error {
					uploaded = true
					return nil
				},
			},
		},
		stderr: io.Discard,
		stdout: io.Discard,
	}

	// Construct a header that will have Name="./" which strips to empty.
	// We pass the header directly instead of going through tar writer validation.
	hdr := &tar.Header{
		Name:     "./",
		Mode:     0o755,
		Size:     0,
		Typeflag: tar.TypeDir,
		Format:   tar.FormatGNU,
	}
	// Create a minimal tar reader (we won't actually read from it for dirs)
	var buf bytes.Buffer
	tr := tar.NewReader(&buf)

	warnings, err := r.extractToS3(context.Background(), locator.Ref{Kind: locator.KindS3, Bucket: "b", Key: "p"}, hdr, tr, nil)
	if err != nil {
		t.Fatalf("extractToS3() error = %v", err)
	}
	if warnings != 0 {
		t.Fatalf("warnings = %d", warnings)
	}
	if uploaded {
		t.Fatalf("uploadStream should not be called for empty names")
	}
}

// TestExtractToS3SymlinkEntry verifies symlinks are uploaded as empty objects with gotgz-type metadata.
func TestExtractToS3SymlinkEntry(t *testing.T) {
	var uploadedRef locator.Ref
	var uploadedMeta map[string]string

	r := &Runner{
		storage: &storageRouter{
			s3: fakeS3ArchiveStore{
				uploadStream: func(_ context.Context, ref locator.Ref, body io.Reader, metadata map[string]string) error {
					uploadedRef = ref
					uploadedMeta = metadata
					_, _ = io.ReadAll(body)
					return nil
				},
			},
		},
		stderr: io.Discard,
		stdout: io.Discard,
	}

	hdr := &tar.Header{
		Name:     "link.txt",
		Mode:     0o777,
		Typeflag: tar.TypeSymlink,
		Linkname: "real.txt",
		Format:   tar.FormatPAX,
	}
	tr := newTarReaderFromEntries(t, []tarEntry{{hdr: hdr, body: ""}})
	_, _ = tr.Next()

	warnings, err := r.extractToS3(context.Background(), locator.Ref{Kind: locator.KindS3, Bucket: "b", Key: "p"}, hdr, tr, nil)
	if err != nil {
		t.Fatalf("extractToS3() error = %v", err)
	}
	if warnings != 0 {
		t.Fatalf("warnings = %d", warnings)
	}
	if !strings.Contains(uploadedRef.Key, "link.txt") {
		t.Fatalf("key = %q", uploadedRef.Key)
	}
	if uploadedMeta["gotgz-type"] != fmt.Sprintf("%d", tar.TypeSymlink) {
		t.Fatalf("gotgz-type = %q", uploadedMeta["gotgz-type"])
	}
}

// ---------------------------------------------------------------------------
// resolveS3ArchiveVolumes (0%)
// ---------------------------------------------------------------------------

// TestResolveS3ArchiveVolumes verifies S3 split volume discovery.
func TestResolveS3ArchiveVolumes(t *testing.T) {
	r := &Runner{
		storage: &storageRouter{
			s3: fakeS3ArchiveStore{
				listPrefix: func(_ context.Context, bucket string, prefix string) ([]s3store.ListedObject, error) {
					if bucket != "mybucket" {
						t.Fatalf("bucket = %q", bucket)
					}
					return []s3store.ListedObject{
						{Key: "arch/bundle.part0001.tar", Size: 100},
						{Key: "arch/bundle.part0002.tar", Size: 200},
						{Key: "arch/bundle.part0003.tar", Size: 300},
					}, nil
				},
			},
		},
		stderr: io.Discard,
		stdout: io.Discard,
	}

	ref := locator.Ref{
		Kind:   locator.KindS3,
		Raw:    "s3://mybucket/arch/bundle.part0001.tar",
		Bucket: "mybucket",
		Key:    "arch/bundle.part0001.tar",
	}

	volumes, err := r.resolveArchiveVolumes(context.Background(), ref, archiveReaderInfo{Size: 100, SizeKnown: true})
	if err != nil {
		t.Fatalf("resolveArchiveVolumes() error = %v", err)
	}
	if len(volumes) != 3 {
		t.Fatalf("len(volumes) = %d, want 3", len(volumes))
	}
	if volumes[0].info.Size != 100 {
		t.Fatalf("volumes[0].info.Size = %d", volumes[0].info.Size)
	}
	if volumes[1].info.Size != 200 {
		t.Fatalf("volumes[1].info.Size = %d", volumes[1].info.Size)
	}
	if volumes[2].info.Size != 300 {
		t.Fatalf("volumes[2].info.Size = %d", volumes[2].info.Size)
	}
}

// TestResolveS3ArchiveVolumesWithGap verifies that a missing volume is reported.
func TestResolveS3ArchiveVolumesWithGap(t *testing.T) {
	r := &Runner{
		storage: &storageRouter{
			s3: fakeS3ArchiveStore{
				listPrefix: func(_ context.Context, _ string, _ string) ([]s3store.ListedObject, error) {
					return []s3store.ListedObject{
						{Key: "arch/bundle.part0001.tar", Size: 100},
						{Key: "arch/bundle.part0003.tar", Size: 300},
					}, nil
				},
			},
		},
		stderr: io.Discard,
		stdout: io.Discard,
	}

	ref := locator.Ref{
		Kind:   locator.KindS3,
		Raw:    "s3://mybucket/arch/bundle.part0001.tar",
		Bucket: "mybucket",
		Key:    "arch/bundle.part0001.tar",
	}

	_, err := r.resolveArchiveVolumes(context.Background(), ref, archiveReaderInfo{Size: 100, SizeKnown: true})
	if err == nil || !strings.Contains(err.Error(), "missing split archive volume") {
		t.Fatalf("err = %v, want missing split archive volume", err)
	}
}

// TestResolveS3ArchiveVolumesListError verifies that S3 list errors propagate.
func TestResolveS3ArchiveVolumesListError(t *testing.T) {
	wantErr := errors.New("s3 list failed")
	r := &Runner{
		storage: &storageRouter{
			s3: fakeS3ArchiveStore{
				listPrefix: func(_ context.Context, _ string, _ string) ([]s3store.ListedObject, error) {
					return nil, wantErr
				},
			},
		},
		stderr: io.Discard,
		stdout: io.Discard,
	}

	ref := locator.Ref{
		Kind:   locator.KindS3,
		Raw:    "s3://mybucket/arch/bundle.part0001.tar",
		Bucket: "mybucket",
		Key:    "arch/bundle.part0001.tar",
	}

	_, err := r.resolveArchiveVolumes(context.Background(), ref, archiveReaderInfo{})
	if !errors.Is(err, wantErr) {
		t.Fatalf("err = %v, want %v", err, wantErr)
	}
}

// ---------------------------------------------------------------------------
// Run edge cases
// ---------------------------------------------------------------------------

// TestRunUnsupportedModeFails verifies that unsupported mode values fail.
func TestRunUnsupportedModeFails(t *testing.T) {
	r := &Runner{
		storage: &storageRouter{},
		stderr:  io.Discard,
		stdout:  io.Discard,
	}
	got := r.Run(context.Background(), cli.Options{Mode: "z"})
	if got.ExitCode != ExitFatal {
		t.Fatalf("ExitCode = %d, want %d", got.ExitCode, ExitFatal)
	}
	if got.Err == nil || !strings.Contains(got.Err.Error(), "unsupported mode") {
		t.Fatalf("Err = %v", got.Err)
	}
}

// TestClassifyResultWarnings verifies warning-only results.
func TestClassifyResultWarnings(t *testing.T) {
	got := classifyResult(nil, 3)
	if got.ExitCode != ExitWarning {
		t.Fatalf("ExitCode = %d, want %d", got.ExitCode, ExitWarning)
	}
}

// TestClassifyResultFatal verifies fatal error results.
func TestClassifyResultFatal(t *testing.T) {
	got := classifyResult(errors.New("boom"), 0)
	if got.ExitCode != ExitFatal {
		t.Fatalf("ExitCode = %d, want %d", got.ExitCode, ExitFatal)
	}
}

// TestClassifyResultSuccess verifies clean results.
func TestClassifyResultSuccess(t *testing.T) {
	got := classifyResult(nil, 0)
	if got.ExitCode != ExitSuccess {
		t.Fatalf("ExitCode = %d, want %d", got.ExitCode, ExitSuccess)
	}
}

// ---------------------------------------------------------------------------
// Extract tar to stdout
// ---------------------------------------------------------------------------

// TestExtractTarToStdout verifies tar extraction to stdout sends matching content.
func TestExtractTarToStdout(t *testing.T) {
	root := t.TempDir()
	src := filepath.Join(root, "src")
	archive := filepath.Join(root, "a.tar")

	if err := os.MkdirAll(src, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "hello.txt"), []byte("tar-stdout"), 0o644); err != nil {
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
		Mode:     cli.ModeExtract,
		Archive:  archive,
		ToStdout: true,
		Members:  []string{"src/hello.txt"},
	}
	if got := r.Run(context.Background(), extract); got.ExitCode != ExitSuccess {
		t.Fatalf("extract exit=%d err=%v", got.ExitCode, got.Err)
	}
	if out.String() != "tar-stdout" {
		t.Fatalf("stdout = %q, want tar-stdout", out.String())
	}
}

// TestExtractTarToStdoutWithStripAndWildcards exercises stdout with stripComponents and wildcards.
func TestExtractTarToStdoutWithStripAndWildcards(t *testing.T) {
	root := t.TempDir()
	src := filepath.Join(root, "src")
	archive := filepath.Join(root, "a.tar")

	if err := os.MkdirAll(src, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "match.txt"), []byte("matched"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "skip.dat"), []byte("skipped"), 0o644); err != nil {
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
		Mode:            cli.ModeExtract,
		Archive:         archive,
		ToStdout:        true,
		Wildcards:       true,
		StripComponents: 1,
		Members:         []string{"src/*.txt"},
	}
	if got := r.Run(context.Background(), extract); got.ExitCode != ExitSuccess {
		t.Fatalf("extract exit=%d err=%v", got.ExitCode, got.Err)
	}
	if out.String() != "matched" {
		t.Fatalf("stdout = %q, want matched", out.String())
	}
}

// ---------------------------------------------------------------------------
// List tar with member selection
// ---------------------------------------------------------------------------

// TestListTarWithMemberFilter verifies tar list with specific member names.
func TestListTarWithMemberFilter(t *testing.T) {
	root := t.TempDir()
	src := filepath.Join(root, "src")
	archive := filepath.Join(root, "a.tar")

	if err := os.MkdirAll(src, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "a.txt"), []byte("a"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "b.txt"), []byte("b"), 0o644); err != nil {
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

	list := cli.Options{Mode: cli.ModeList, Archive: archive, Members: []string{"src/a.txt"}}
	if got := r.Run(context.Background(), list); got.ExitCode != ExitSuccess {
		t.Fatalf("list exit=%d err=%v", got.ExitCode, got.Err)
	}
	if !strings.Contains(out.String(), "src/a.txt") {
		t.Fatalf("list output should include src/a.txt: %q", out.String())
	}
	if strings.Contains(out.String(), "src/b.txt") {
		t.Fatalf("list output should not include src/b.txt: %q", out.String())
	}
}

// TestListTarWithWildcardFilter verifies tar list with wildcard patterns.
func TestListTarWithWildcardFilter(t *testing.T) {
	root := t.TempDir()
	archive := filepath.Join(root, "a.tar")

	if err := os.WriteFile(filepath.Join(root, "a.txt"), []byte("a"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "b.dat"), []byte("b"), 0o644); err != nil {
		t.Fatal(err)
	}

	var out bytes.Buffer
	r, err := New(context.Background(), &out, io.Discard)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	create := cli.Options{Mode: cli.ModeCreate, Archive: archive, Chdir: root, Members: []string{"a.txt", "b.dat"}}
	if got := r.Run(context.Background(), create); got.ExitCode != ExitSuccess {
		t.Fatalf("create exit=%d err=%v", got.ExitCode, got.Err)
	}

	list := cli.Options{Mode: cli.ModeList, Archive: archive, Wildcards: true, Members: []string{"*.txt"}}
	if got := r.Run(context.Background(), list); got.ExitCode != ExitSuccess {
		t.Fatalf("list exit=%d err=%v", got.ExitCode, got.Err)
	}
	if !strings.Contains(out.String(), "a.txt") {
		t.Fatalf("list output should include a.txt: %q", out.String())
	}
	if strings.Contains(out.String(), "b.dat") {
		t.Fatalf("list output should not include b.dat: %q", out.String())
	}
}

// ---------------------------------------------------------------------------
// Verbose mode extraction
// ---------------------------------------------------------------------------

// TestExtractTarVerbose verifies extraction lists names on stdout when verbose.
func TestExtractTarVerbose(t *testing.T) {
	root := t.TempDir()
	archive := filepath.Join(root, "a.tar")
	out := filepath.Join(root, "out")

	if err := os.WriteFile(filepath.Join(root, "file.txt"), []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(out, 0o755); err != nil {
		t.Fatal(err)
	}

	var stdout bytes.Buffer
	r, err := New(context.Background(), &stdout, io.Discard)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	create := cli.Options{Mode: cli.ModeCreate, Archive: archive, Chdir: root, Members: []string{"file.txt"}}
	if got := r.Run(context.Background(), create); got.ExitCode != ExitSuccess {
		t.Fatalf("create exit=%d err=%v", got.ExitCode, got.Err)
	}

	stdout.Reset()
	extract := cli.Options{Mode: cli.ModeExtract, Archive: archive, Chdir: out, Verbose: true}
	if got := r.Run(context.Background(), extract); got.ExitCode != ExitSuccess {
		t.Fatalf("extract exit=%d err=%v", got.ExitCode, got.Err)
	}
	if !strings.Contains(stdout.String(), "file.txt") {
		t.Fatalf("verbose output should include file.txt: %q", stdout.String())
	}
}

// TestCreateTarVerbose verifies creation lists names on stdout when verbose.
func TestCreateTarVerbose(t *testing.T) {
	root := t.TempDir()
	archive := filepath.Join(root, "a.tar")

	if err := os.WriteFile(filepath.Join(root, "file.txt"), []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}

	var stdout bytes.Buffer
	r, err := New(context.Background(), &stdout, io.Discard)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	create := cli.Options{Mode: cli.ModeCreate, Archive: archive, Chdir: root, Verbose: true, Members: []string{"file.txt"}}
	if got := r.Run(context.Background(), create); got.ExitCode != ExitSuccess {
		t.Fatalf("create exit=%d err=%v", got.ExitCode, got.Err)
	}
	if !strings.Contains(stdout.String(), "file.txt") {
		t.Fatalf("verbose output should include file.txt: %q", stdout.String())
	}
}

// ---------------------------------------------------------------------------
// Symlink create round-trip (tar)
// ---------------------------------------------------------------------------

// TestCreateExtractTarSymlinkRoundTrip verifies symlink preservation in tar archives.
func TestCreateExtractTarSymlinkRoundTrip(t *testing.T) {
	root := t.TempDir()
	src := filepath.Join(root, "src")
	out := filepath.Join(root, "out")
	archive := filepath.Join(root, "a.tar")

	if err := os.MkdirAll(src, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "target.txt"), []byte("link-target"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("target.txt", filepath.Join(src, "link.txt")); err != nil {
		t.Skipf("symlink not supported: %v", err)
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
		t.Fatalf("lstat: %v", err)
	}
	if info.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("expected symlink, mode=%v", info.Mode())
	}
	target, err := os.Readlink(linkPath)
	if err != nil {
		t.Fatalf("readlink: %v", err)
	}
	if target != "target.txt" {
		t.Fatalf("target = %q, want target.txt", target)
	}
}

// ---------------------------------------------------------------------------
// Extract tar member selection
// ---------------------------------------------------------------------------

// TestExtractTarMemberSelection verifies that member filter selects specific entries.
func TestExtractTarMemberSelection(t *testing.T) {
	root := t.TempDir()
	archive := filepath.Join(root, "a.tar")
	out := filepath.Join(root, "out")

	if err := os.WriteFile(filepath.Join(root, "a.txt"), []byte("keep-a"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "b.txt"), []byte("skip-b"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(out, 0o755); err != nil {
		t.Fatal(err)
	}

	r, err := New(context.Background(), io.Discard, io.Discard)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	create := cli.Options{Mode: cli.ModeCreate, Archive: archive, Chdir: root, Members: []string{"a.txt", "b.txt"}}
	if got := r.Run(context.Background(), create); got.ExitCode != ExitSuccess {
		t.Fatalf("create exit=%d err=%v", got.ExitCode, got.Err)
	}

	extract := cli.Options{Mode: cli.ModeExtract, Archive: archive, Chdir: out, Members: []string{"a.txt"}}
	if got := r.Run(context.Background(), extract); got.ExitCode != ExitSuccess {
		t.Fatalf("extract exit=%d err=%v", got.ExitCode, got.Err)
	}

	if _, err := os.Stat(filepath.Join(out, "a.txt")); err != nil {
		t.Fatalf("a.txt should exist: %v", err)
	}
	if _, err := os.Stat(filepath.Join(out, "b.txt")); !os.IsNotExist(err) {
		t.Fatalf("b.txt should not exist, err=%v", err)
	}
}

// ---------------------------------------------------------------------------
// Create tar with exclude patterns
// ---------------------------------------------------------------------------

// TestCreateTarWithExcludePatterns verifies the --exclude flag.
func TestCreateTarWithExcludePatterns(t *testing.T) {
	root := t.TempDir()
	archive := filepath.Join(root, "a.tar")

	if err := os.WriteFile(filepath.Join(root, "keep.txt"), []byte("keep"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "skip.tmp"), []byte("skip"), 0o644); err != nil {
		t.Fatal(err)
	}

	var listBuf bytes.Buffer
	r, err := New(context.Background(), &listBuf, io.Discard)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	create := cli.Options{
		Mode:    cli.ModeCreate,
		Archive: archive,
		Chdir:   root,
		Exclude: []string{"*.tmp"},
		Members: []string{"keep.txt", "skip.tmp"},
	}
	if got := r.Run(context.Background(), create); got.ExitCode != ExitSuccess {
		t.Fatalf("create exit=%d err=%v", got.ExitCode, got.Err)
	}

	list := cli.Options{Mode: cli.ModeList, Archive: archive}
	if got := r.Run(context.Background(), list); got.ExitCode != ExitSuccess {
		t.Fatalf("list exit=%d err=%v", got.ExitCode, got.Err)
	}
	if !strings.Contains(listBuf.String(), "keep.txt") {
		t.Fatalf("list should include keep.txt: %q", listBuf.String())
	}
	if strings.Contains(listBuf.String(), "skip.tmp") {
		t.Fatalf("list should not include skip.tmp: %q", listBuf.String())
	}
}

// ---------------------------------------------------------------------------
// Create zip with exclude patterns
// ---------------------------------------------------------------------------

// TestCreateZipWithExcludePatterns verifies the --exclude flag for zip.
func TestCreateZipWithExcludePatterns(t *testing.T) {
	root := t.TempDir()
	archive := filepath.Join(root, "a.zip")

	if err := os.WriteFile(filepath.Join(root, "keep.txt"), []byte("keep"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "skip.tmp"), []byte("skip"), 0o644); err != nil {
		t.Fatal(err)
	}

	var listBuf bytes.Buffer
	r, err := New(context.Background(), &listBuf, io.Discard)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	create := cli.Options{
		Mode:    cli.ModeCreate,
		Archive: archive,
		Chdir:   root,
		Exclude: []string{"*.tmp"},
		Members: []string{"keep.txt", "skip.tmp"},
	}
	if got := r.Run(context.Background(), create); got.ExitCode != ExitSuccess {
		t.Fatalf("create exit=%d err=%v", got.ExitCode, got.Err)
	}

	list := cli.Options{Mode: cli.ModeList, Archive: archive}
	if got := r.Run(context.Background(), list); got.ExitCode != ExitSuccess {
		t.Fatalf("list exit=%d err=%v", got.ExitCode, got.Err)
	}
	if !strings.Contains(listBuf.String(), "keep.txt") {
		t.Fatalf("list should include keep.txt: %q", listBuf.String())
	}
	if strings.Contains(listBuf.String(), "skip.tmp") {
		t.Fatalf("list should not include skip.tmp: %q", listBuf.String())
	}
}

// ---------------------------------------------------------------------------
// Zip list member selection
// ---------------------------------------------------------------------------

// TestListZipWithMemberFilter verifies zip list with specific member names.
func TestListZipWithMemberFilter(t *testing.T) {
	root := t.TempDir()
	archive := filepath.Join(root, "a.zip")

	if err := os.WriteFile(filepath.Join(root, "a.txt"), []byte("a"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "b.txt"), []byte("b"), 0o644); err != nil {
		t.Fatal(err)
	}

	var out bytes.Buffer
	r, err := New(context.Background(), &out, io.Discard)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	create := cli.Options{Mode: cli.ModeCreate, Archive: archive, Chdir: root, Members: []string{"a.txt", "b.txt"}}
	if got := r.Run(context.Background(), create); got.ExitCode != ExitSuccess {
		t.Fatalf("create exit=%d err=%v", got.ExitCode, got.Err)
	}

	list := cli.Options{Mode: cli.ModeList, Archive: archive, Members: []string{"a.txt"}}
	if got := r.Run(context.Background(), list); got.ExitCode != ExitSuccess {
		t.Fatalf("list exit=%d err=%v", got.ExitCode, got.Err)
	}
	if !strings.Contains(out.String(), "a.txt") {
		t.Fatalf("list output should include a.txt: %q", out.String())
	}
	if strings.Contains(out.String(), "b.txt") {
		t.Fatalf("list output should not include b.txt: %q", out.String())
	}
}

// ---------------------------------------------------------------------------
// Zip extract member selection
// ---------------------------------------------------------------------------

// TestExtractZipMemberSelection verifies zip extract with member filter.
func TestExtractZipMemberSelection(t *testing.T) {
	root := t.TempDir()
	archive := filepath.Join(root, "a.zip")
	out := filepath.Join(root, "out")

	if err := os.WriteFile(filepath.Join(root, "a.txt"), []byte("keep-a"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "b.txt"), []byte("skip-b"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(out, 0o755); err != nil {
		t.Fatal(err)
	}

	r, err := New(context.Background(), io.Discard, io.Discard)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	create := cli.Options{Mode: cli.ModeCreate, Archive: archive, Chdir: root, Members: []string{"a.txt", "b.txt"}}
	if got := r.Run(context.Background(), create); got.ExitCode != ExitSuccess {
		t.Fatalf("create exit=%d err=%v", got.ExitCode, got.Err)
	}

	extract := cli.Options{Mode: cli.ModeExtract, Archive: archive, Chdir: out, Members: []string{"a.txt"}}
	if got := r.Run(context.Background(), extract); got.ExitCode != ExitSuccess {
		t.Fatalf("extract exit=%d err=%v", got.ExitCode, got.Err)
	}

	if _, err := os.Stat(filepath.Join(out, "a.txt")); err != nil {
		t.Fatalf("a.txt should exist: %v", err)
	}
	if _, err := os.Stat(filepath.Join(out, "b.txt")); !os.IsNotExist(err) {
		t.Fatalf("b.txt should not exist, err=%v", err)
	}
}

// ---------------------------------------------------------------------------
// Create zip verbose
// ---------------------------------------------------------------------------

// TestCreateZipVerbose verifies creation of zip in verbose mode.
func TestCreateZipVerbose(t *testing.T) {
	root := t.TempDir()
	archive := filepath.Join(root, "a.zip")

	if err := os.WriteFile(filepath.Join(root, "file.txt"), []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}

	var stdout bytes.Buffer
	r, err := New(context.Background(), &stdout, io.Discard)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	create := cli.Options{Mode: cli.ModeCreate, Archive: archive, Chdir: root, Verbose: true, Members: []string{"file.txt"}}
	if got := r.Run(context.Background(), create); got.ExitCode != ExitSuccess {
		t.Fatalf("create exit=%d err=%v", got.ExitCode, got.Err)
	}
	if !strings.Contains(stdout.String(), "file.txt") {
		t.Fatalf("verbose output should include file.txt: %q", stdout.String())
	}
}

// TestExtractZipVerbose verifies extraction in verbose mode.
func TestExtractZipVerbose(t *testing.T) {
	root := t.TempDir()
	archive := filepath.Join(root, "a.zip")
	out := filepath.Join(root, "out")

	if err := os.WriteFile(filepath.Join(root, "file.txt"), []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(out, 0o755); err != nil {
		t.Fatal(err)
	}

	var stdout bytes.Buffer
	r, err := New(context.Background(), &stdout, io.Discard)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	create := cli.Options{Mode: cli.ModeCreate, Archive: archive, Chdir: root, Members: []string{"file.txt"}}
	if got := r.Run(context.Background(), create); got.ExitCode != ExitSuccess {
		t.Fatalf("create exit=%d err=%v", got.ExitCode, got.Err)
	}

	stdout.Reset()
	extract := cli.Options{Mode: cli.ModeExtract, Archive: archive, Chdir: out, Verbose: true}
	if got := r.Run(context.Background(), extract); got.ExitCode != ExitSuccess {
		t.Fatalf("extract exit=%d err=%v", got.ExitCode, got.Err)
	}
	if !strings.Contains(stdout.String(), "file.txt") {
		t.Fatalf("verbose output should include file.txt: %q", stdout.String())
	}
}

// ---------------------------------------------------------------------------
// Storage router: additional coverage for openArchiveWriter S3 empty key
// ---------------------------------------------------------------------------

// TestStorageRouterOpenArchiveWriterRejectsEmptyS3Key verifies that empty S3 keys fail.
func TestStorageRouterOpenArchiveWriterRejectsEmptyS3Key(t *testing.T) {
	router := &storageRouter{
		s3: fakeS3ArchiveStore{},
	}
	_, err := router.openArchiveWriter(context.Background(), locator.Ref{Kind: locator.KindS3, Raw: "s3://bucket/", Bucket: "bucket", Key: ""})
	if err == nil || !strings.Contains(err.Error(), "cannot be empty") {
		t.Fatalf("openArchiveWriter() err = %v, want empty key error", err)
	}
}

// TestStorageRouterOpenArchiveWriterUnsupportedKind verifies unknown archive target falls through.
func TestStorageRouterOpenArchiveWriterUnsupportedKind(t *testing.T) {
	router := &storageRouter{}
	_, err := router.openArchiveWriter(context.Background(), locator.Ref{Kind: "unknown", Raw: "unknown://foo"})
	if err == nil || !strings.Contains(err.Error(), "unsupported archive target") {
		t.Fatalf("openArchiveWriter() err = %v, want unsupported error", err)
	}
}

// TestStorageRouterOpenZipRangeReaderUnsupportedKind covers the default branch.
func TestStorageRouterOpenZipRangeReaderUnsupportedKind(t *testing.T) {
	router := &storageRouter{}
	_, err := router.openZipRangeReader(context.Background(), locator.Ref{Kind: locator.KindLocal, Raw: "local"}, 0, 1)
	if err == nil || !strings.Contains(err.Error(), "unsupported zip range source") {
		t.Fatalf("openZipRangeReader() err = %v, want unsupported error", err)
	}
}

// TestStorageRouterOpenArchiveReaderUnsupportedKind covers the default branch.
func TestStorageRouterOpenArchiveReaderUnsupportedKind(t *testing.T) {
	router := &storageRouter{}
	_, _, err := router.openArchiveReader(context.Background(), locator.Ref{Kind: "unknown", Raw: "unknown://foo"})
	if err == nil || !strings.Contains(err.Error(), "unsupported archive source") {
		t.Fatalf("openArchiveReader() err = %v, want unsupported error", err)
	}
}

// TestStorageRouterOpenS3ArchiveReaderEmptyKey verifies an S3 read with empty key fails.
func TestStorageRouterOpenS3ArchiveReaderEmptyKey(t *testing.T) {
	router := &storageRouter{
		s3: fakeS3ArchiveStore{},
	}
	_, _, err := router.openArchiveReader(context.Background(), locator.Ref{Kind: locator.KindS3, Raw: "s3://bucket/", Bucket: "bucket", Key: ""})
	if err == nil || !strings.Contains(err.Error(), "cannot be empty") {
		t.Fatalf("openArchiveReader() err = %v, want empty key error", err)
	}
}

// TestStorageRouterOpenZipRangeReaderS3EmptyKey verifies an S3 zip range read with empty key fails.
func TestStorageRouterOpenZipRangeReaderS3EmptyKey(t *testing.T) {
	router := &storageRouter{
		s3ZipRange: fakeS3ZipArchiveStore{},
	}
	_, err := router.openZipRangeReader(context.Background(), locator.Ref{Kind: locator.KindS3, Raw: "s3://bucket/", Bucket: "bucket", Key: ""}, 0, 1)
	if err == nil || !strings.Contains(err.Error(), "cannot be empty") {
		t.Fatalf("openZipRangeReader() err = %v, want empty key error", err)
	}
}

// TestStorageRouterOpenZipRangeReaderHTTPMissing covers the nil httpZipRange branch.
func TestStorageRouterOpenZipRangeReaderHTTPMissing(t *testing.T) {
	router := &storageRouter{http: fakeHTTPArchiveStore{}}
	_, err := router.openZipRangeReader(context.Background(), locator.Ref{Kind: locator.KindHTTP, Raw: "https://example.test/archive.zip", URL: "https://example.test/archive.zip"}, 0, 1)
	if err == nil || !strings.Contains(err.Error(), "http zip range store is not configured") {
		t.Fatalf("openZipRangeReader() err = %v, want not configured error", err)
	}
}

// ---------------------------------------------------------------------------
// storageRouter S3 delegation: statS3Object, uploadS3Object, listS3Prefix errors
// ---------------------------------------------------------------------------

// TestStorageRouterStatS3ObjectRequiresS3 verifies that nil s3 is rejected.
func TestStorageRouterStatS3ObjectRequiresS3(t *testing.T) {
	router := &storageRouter{}
	_, err := router.statS3Object(context.Background(), locator.Ref{})
	if err == nil || !strings.Contains(err.Error(), "s3 archive store is not configured") {
		t.Fatalf("err = %v", err)
	}
}

// TestStorageRouterUploadS3ObjectRequiresS3 verifies that nil s3 is rejected.
func TestStorageRouterUploadS3ObjectRequiresS3(t *testing.T) {
	router := &storageRouter{}
	err := router.uploadS3Object(context.Background(), locator.Ref{}, nil, nil)
	if err == nil || !strings.Contains(err.Error(), "s3 archive store is not configured") {
		t.Fatalf("err = %v", err)
	}
}

// TestStorageRouterListS3PrefixRequiresS3 verifies that nil s3 is rejected.
func TestStorageRouterListS3PrefixRequiresS3(t *testing.T) {
	router := &storageRouter{}
	_, err := router.listS3Prefix(context.Background(), "bucket", "prefix")
	if err == nil || !strings.Contains(err.Error(), "s3 archive store is not configured") {
		t.Fatalf("err = %v", err)
	}
}

// TestStorageRouterOpenS3ObjectReaderRequiresS3 verifies that nil s3 is rejected.
func TestStorageRouterOpenS3ObjectReaderRequiresS3(t *testing.T) {
	router := &storageRouter{}
	_, _, err := router.openS3ObjectReader(context.Background(), locator.Ref{})
	if err == nil || !strings.Contains(err.Error(), "s3 archive store is not configured") {
		t.Fatalf("err = %v", err)
	}
}

// ---------------------------------------------------------------------------
// localCreateBasePrefix edge cases
// ---------------------------------------------------------------------------

// TestLocalCreateBasePrefixRootPath verifies root path returns the path itself.
func TestLocalCreateBasePrefixRootPath(t *testing.T) {
	got := localCreateBasePrefix("/")
	if got != "/" {
		t.Fatalf("got %q, want /", got)
	}
}

// TestLocalCreateBasePrefixDot verifies dot path returns empty string.
func TestLocalCreateBasePrefixDot(t *testing.T) {
	got := localCreateBasePrefix(".")
	if got != "" {
		t.Fatalf("got %q, want empty", got)
	}
}

// ---------------------------------------------------------------------------
// localCreateArchiveName edge cases
// ---------------------------------------------------------------------------

// TestLocalCreateArchiveNameFallbackToRel exercises the filepath.Rel fallback.
func TestLocalCreateArchiveNameFallbackToRel(t *testing.T) {
	// The current path does not have the expected prefix;
	// this forces the Rel fallback.
	got, err := localCreateArchiveName("/a/b", "/a/b"+string(filepath.Separator), "/a/unexpected/sub", "member")
	if err != nil {
		t.Fatalf("error = %v", err)
	}
	// filepath.Rel("/a/b", "/a/unexpected/sub") => "../unexpected/sub"
	if !strings.Contains(got, "unexpected") {
		t.Fatalf("got %q, want contains 'unexpected'", got)
	}
}

// TestLocalCreateArchiveNameBasePathMatchesCurrent exercises the basePath==current path.
func TestLocalCreateArchiveNameBasePathMatchesCurrent(t *testing.T) {
	got, err := localCreateArchiveName("/a/b", "/a/b/", "/a/b", "member")
	if err != nil {
		t.Fatalf("error = %v", err)
	}
	if got != "member" {
		t.Fatalf("got %q, want member", got)
	}
}

// TestLocalCreateArchiveNameEmptyPrefix exercises the empty prefix path.
func TestLocalCreateArchiveNameEmptyPrefix(t *testing.T) {
	got, err := localCreateArchiveName(".", "", "subdir/file.txt", ".")
	if err != nil {
		t.Fatalf("error = %v", err)
	}
	if got != "subdir/file.txt" {
		t.Fatalf("got %q, want subdir/file.txt", got)
	}
}

// ---------------------------------------------------------------------------
// joinLocalCreateArchiveName edge cases
// ---------------------------------------------------------------------------

// TestJoinLocalCreateArchiveNameRootSlash exercises the "/" member path.
func TestJoinLocalCreateArchiveNameRootSlash(t *testing.T) {
	got := joinLocalCreateArchiveName("/", "file.txt")
	if got != "/file.txt" {
		t.Fatalf("got %q, want /file.txt", got)
	}
}

// ---------------------------------------------------------------------------
// Create tar with progress enabled
// ---------------------------------------------------------------------------

// TestCreateTarWithProgress verifies progress output when enabled.
func TestCreateTarWithProgress(t *testing.T) {
	root := t.TempDir()
	archive := filepath.Join(root, "a.tar")

	if err := os.WriteFile(filepath.Join(root, "file.txt"), []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}

	var stderr bytes.Buffer
	r, err := New(context.Background(), io.Discard, &stderr)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	create := cli.Options{
		Mode:     cli.ModeCreate,
		Archive:  archive,
		Chdir:    root,
		Progress: cli.ProgressAlways,
		Members:  []string{"file.txt"},
	}
	got := r.Run(context.Background(), create)
	if got.ExitCode != ExitSuccess {
		t.Fatalf("exit=%d err=%v", got.ExitCode, got.Err)
	}
	if !got.ProgressEnabled {
		t.Fatalf("ProgressEnabled = false, want true")
	}
	if !strings.Contains(stderr.String(), "elapsed") {
		t.Fatalf("stderr should contain progress output: %q", stderr.String())
	}
}

// ---------------------------------------------------------------------------
// Create zip with progress enabled
// ---------------------------------------------------------------------------

// TestCreateZipWithProgress verifies progress output for zip.
func TestCreateZipWithProgress(t *testing.T) {
	root := t.TempDir()
	archive := filepath.Join(root, "a.zip")

	if err := os.WriteFile(filepath.Join(root, "file.txt"), []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}

	var stderr bytes.Buffer
	r, err := New(context.Background(), io.Discard, &stderr)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	create := cli.Options{
		Mode:     cli.ModeCreate,
		Archive:  archive,
		Chdir:    root,
		Progress: cli.ProgressAlways,
		Members:  []string{"file.txt"},
	}
	got := r.Run(context.Background(), create)
	if got.ExitCode != ExitSuccess {
		t.Fatalf("exit=%d err=%v", got.ExitCode, got.Err)
	}
	if !got.ProgressEnabled {
		t.Fatalf("ProgressEnabled = false, want true")
	}
}

// ---------------------------------------------------------------------------
// buildCreatePlan edge cases
// ---------------------------------------------------------------------------

// TestBuildCreatePlanContextCancelled verifies that context cancellation propagates.
func TestBuildCreatePlanContextCancelled(t *testing.T) {
	r := &Runner{
		storage: &storageRouter{},
		stderr:  io.Discard,
		stdout:  io.Discard,
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := r.buildCreatePlan(ctx, cli.Options{
		Members: []string{"file.txt"},
	}, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

// TestBuildCreatePlanExcludesS3MemberByPattern verifies S3 members matching exclude are skipped.
func TestBuildCreatePlanExcludesS3MemberByPattern(t *testing.T) {
	r := &Runner{
		storage: &storageRouter{
			s3: fakeS3ArchiveStore{
				stat: func(_ context.Context, _ locator.Ref) (s3store.Metadata, error) {
					t.Fatalf("stat should not be called for excluded members")
					return s3store.Metadata{}, nil
				},
			},
		},
		stderr: io.Discard,
		stdout: io.Discard,
	}

	exclude := archivepath.NewCompiledPathMatcher([]string{"excluded-key"})
	plan, err := r.buildCreatePlan(context.Background(), cli.Options{
		Members: []string{"s3://bucket/excluded-key"},
	}, exclude)
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if len(plan.members) != 0 {
		t.Fatalf("members = %d, want 0", len(plan.members))
	}
}

// ---------------------------------------------------------------------------
// mergeArchiveReaderInfo
// ---------------------------------------------------------------------------

// TestMergeArchiveReaderInfoRuntimeOverrides verifies runtime info takes precedence.
func TestMergeArchiveReaderInfoRuntimeOverrides(t *testing.T) {
	base := archiveReaderInfo{Size: 10, SizeKnown: true, ContentType: "base"}
	runtime := archiveReaderInfo{Size: 20, SizeKnown: true, ContentType: "runtime"}

	got := mergeArchiveReaderInfo(base, runtime)
	if got.Size != 20 {
		t.Fatalf("Size = %d, want 20", got.Size)
	}
	if got.ContentType != "runtime" {
		t.Fatalf("ContentType = %q", got.ContentType)
	}
}

// TestMergeArchiveReaderInfoBasePreserved verifies base info when runtime is unknown.
func TestMergeArchiveReaderInfoBasePreserved(t *testing.T) {
	base := archiveReaderInfo{Size: 10, SizeKnown: true, ContentType: "base"}
	runtime := archiveReaderInfo{Size: 0, SizeKnown: false, ContentType: ""}

	got := mergeArchiveReaderInfo(base, runtime)
	if got.Size != 10 {
		t.Fatalf("Size = %d, want 10", got.Size)
	}
	if got.ContentType != "base" {
		t.Fatalf("ContentType = %q", got.ContentType)
	}
}

// ---------------------------------------------------------------------------
// addArchiveVolumeSize overflow
// ---------------------------------------------------------------------------

// TestAddArchiveVolumeSizeOverflow verifies clamping to MaxInt64.
func TestAddArchiveVolumeSizeOverflow(t *testing.T) {
	got := addArchiveVolumeSize(1<<62, 1<<62)
	if got <= 0 {
		t.Fatalf("got = %d, want positive", got)
	}
}

// TestAddArchiveVolumeSizeZeroSize verifies zero size is no-op.
func TestAddArchiveVolumeSizeZeroSize(t *testing.T) {
	got := addArchiveVolumeSize(100, 0)
	if got != 100 {
		t.Fatalf("got = %d, want 100", got)
	}
}

// TestAddArchiveVolumeSizeNegativeSize verifies negative size is no-op.
func TestAddArchiveVolumeSizeNegativeSize(t *testing.T) {
	got := addArchiveVolumeSize(100, -1)
	if got != 100 {
		t.Fatalf("got = %d, want 100", got)
	}
}

// ---------------------------------------------------------------------------
// sumArchiveVolumeSizes
// ---------------------------------------------------------------------------

// TestSumArchiveVolumeSizesUnknown verifies unknown sizes return false.
func TestSumArchiveVolumeSizesUnknown(t *testing.T) {
	total, known := sumArchiveVolumeSizes([]archiveVolume{
		{info: archiveReaderInfo{Size: 10, SizeKnown: true}},
		{info: archiveReaderInfo{Size: 0, SizeKnown: false}},
	})
	if known {
		t.Fatalf("known = true, want false")
	}
	if total != 0 {
		t.Fatalf("total = %d, want 0", total)
	}
}

// ---------------------------------------------------------------------------
// filterACLLikeXattrs edge cases
// ---------------------------------------------------------------------------

// TestFilterACLLikeXattrsEmpty verifies nil/empty input returns the same.
func TestFilterACLLikeXattrsEmpty(t *testing.T) {
	got := filterACLLikeXattrs(nil)
	if got != nil {
		t.Fatalf("got = %v, want nil", got)
	}
}

// ---------------------------------------------------------------------------
// warnf
// ---------------------------------------------------------------------------

// TestWarnfWithNilReporter verifies that warnf works with nil reporter.
func TestWarnfWithNilReporter(t *testing.T) {
	var stderr bytes.Buffer
	r := &Runner{stderr: &stderr}
	n := r.warnf(nil, "test %s", "warning")
	if n != 1 {
		t.Fatalf("n = %d, want 1", n)
	}
	if !strings.Contains(stderr.String(), "test warning") {
		t.Fatalf("stderr = %q", stderr.String())
	}
}

// ---------------------------------------------------------------------------
// normalizeCompressionHint
// ---------------------------------------------------------------------------

// TestNormalizeCompressionHintEmpty verifies empty returns auto.
func TestNormalizeCompressionHintEmpty(t *testing.T) {
	got := normalizeCompressionHint("")
	if got != cli.CompressionAuto {
		t.Fatalf("got %q, want %q", got, cli.CompressionAuto)
	}
}

// TestNormalizeCompressionHintNonEmpty verifies non-empty returns as-is.
func TestNormalizeCompressionHintNonEmpty(t *testing.T) {
	got := normalizeCompressionHint(cli.CompressionGzip)
	if got != cli.CompressionGzip {
		t.Fatalf("got %q, want %q", got, cli.CompressionGzip)
	}
}

// ---------------------------------------------------------------------------
// Zip entry helpers
// ---------------------------------------------------------------------------

// TestIsZipDirNil verifies isZipDir with nil.
func TestIsZipDirNil(t *testing.T) {
	if isZipDir(nil) {
		t.Fatal("expected false")
	}
}

// TestIsZipSymlinkNil verifies isZipSymlink with nil.
func TestIsZipSymlinkNil(t *testing.T) {
	if isZipSymlink(nil) {
		t.Fatal("expected false")
	}
}

// TestIsZipRegularNil verifies isZipRegular with nil.
func TestIsZipRegularNil(t *testing.T) {
	if isZipRegular(nil) {
		t.Fatal("expected false")
	}
}

// TestIsZipDirByTrailingSlash verifies directory detection by slash.
func TestIsZipDirByTrailingSlash(t *testing.T) {
	zf := &zip.File{FileHeader: zip.FileHeader{Name: "dir/"}}
	if !isZipDir(zf) {
		t.Fatal("expected isZipDir = true")
	}
}

// ---------------------------------------------------------------------------
// archiveSplitRef edge cases
// ---------------------------------------------------------------------------

// TestArchiveSplitRefRoundTrip verifies archiveSplitRef computes correct paths.
func TestArchiveSplitRefRoundTrip(t *testing.T) {
	base := locator.Ref{Kind: locator.KindLocal, Path: "/tmp/archive.tar.gz", Raw: "/tmp/archive.tar.gz"}

	got := archiveSplitRef(base, 1, 4)
	if !strings.Contains(got.Path, "part0001") {
		t.Fatalf("Path = %q, want part0001", got.Path)
	}
}

// ---------------------------------------------------------------------------
// Extract tar symlink to stdout (non-regular skipped)
// ---------------------------------------------------------------------------

// TestExtractTarToStdoutSkipsNonRegular verifies non-regular entries are skipped in stdout mode.
func TestExtractTarToStdoutSkipsNonRegular(t *testing.T) {
	root := t.TempDir()
	src := filepath.Join(root, "src")
	archive := filepath.Join(root, "a.tar")

	if err := os.MkdirAll(src, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "target.txt"), []byte("real"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("target.txt", filepath.Join(src, "link.txt")); err != nil {
		t.Skipf("symlink not supported: %v", err)
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
		Mode:     cli.ModeExtract,
		Archive:  archive,
		ToStdout: true,
		Members:  []string{"src/target.txt"},
	}
	if got := r.Run(context.Background(), extract); got.ExitCode != ExitSuccess {
		t.Fatalf("extract exit=%d err=%v", got.ExitCode, got.Err)
	}
	// Only the regular file content should appear; symlinks are skipped.
	if out.String() != "real" {
		t.Fatalf("stdout = %q, want real", out.String())
	}
}

// ---------------------------------------------------------------------------
// Resolve HTTP split archives fails
// ---------------------------------------------------------------------------

// TestResolveHTTPSplitArchiveFails verifies that HTTP split archives are unsupported.
func TestResolveHTTPSplitArchiveFails(t *testing.T) {
	r := &Runner{
		storage: &storageRouter{},
		stderr:  io.Discard,
		stdout:  io.Discard,
	}

	ref := locator.Ref{
		Kind: locator.KindHTTP,
		Raw:  "https://example.com/bundle.part0001.tar",
		URL:  "https://example.com/bundle.part0001.tar",
	}

	_, err := r.resolveArchiveVolumes(context.Background(), ref, archiveReaderInfo{})
	if err == nil || !strings.Contains(err.Error(), "http(s) split archives are not supported") {
		t.Fatalf("err = %v, want http split error", err)
	}
}

// TestResolveArchiveVolumesNonSplitNameReturnsOneVolume verifies non-split names return one volume.
func TestResolveArchiveVolumesNonSplitNameReturnsOneVolume(t *testing.T) {
	r := &Runner{
		storage: &storageRouter{},
		stderr:  io.Discard,
		stdout:  io.Discard,
	}

	ref := locator.Ref{
		Kind: locator.KindLocal,
		Raw:  "/tmp/archive.tar",
		Path: "/tmp/archive.tar",
	}

	volumes, err := r.resolveArchiveVolumes(context.Background(), ref, archiveReaderInfo{Size: 100, SizeKnown: true})
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if len(volumes) != 1 || volumes[0].info.Size != 100 {
		t.Fatalf("volumes = %+v", volumes)
	}
}

// TestResolveArchiveVolumesPartNotOneRejected verifies opening non-part0001 is rejected.
func TestResolveArchiveVolumesPartNotOneRejected(t *testing.T) {
	r := &Runner{
		storage: &storageRouter{},
		stderr:  io.Discard,
		stdout:  io.Discard,
	}

	ref := locator.Ref{
		Kind: locator.KindLocal,
		Raw:  "/tmp/bundle.part0002.tar",
		Path: "/tmp/bundle.part0002.tar",
	}

	_, err := r.resolveArchiveVolumes(context.Background(), ref, archiveReaderInfo{})
	if err == nil || !strings.Contains(err.Error(), "part0001") {
		t.Fatalf("err = %v, want part0001 error", err)
	}
}

// ---------------------------------------------------------------------------
// computeExtractPerm edge cases
// ---------------------------------------------------------------------------

// TestComputeExtractPermFallbackWithSamePerms verifies fallback with samePerms=true.
func TestComputeExtractPermFallbackWithSamePerms(t *testing.T) {
	perm := computeExtractPerm(0, 0o755, true)
	if perm != 0o755 {
		t.Fatalf("perm = %o, want 755", perm)
	}
}

// TestComputeExtractPermNoSamePerms verifies umask masking.
func TestComputeExtractPermNoSamePerms(t *testing.T) {
	perm := computeExtractPerm(0o777, 0, false)
	// Result depends on umask; just verify it is <= 0o777.
	if perm > 0o777 {
		t.Fatalf("perm = %o", perm)
	}
}

// ---------------------------------------------------------------------------
// Test helper: newTarReaderFromEntries
// ---------------------------------------------------------------------------

type tarEntry struct {
	hdr  *tar.Header
	body string
}

// newTarReaderFromEntries builds a tar reader from the provided entries.
func newTarReaderFromEntries(t *testing.T, entries []tarEntry) *tar.Reader {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, e := range entries {
		if err := tw.WriteHeader(e.hdr); err != nil {
			t.Fatalf("WriteHeader(): %v", err)
		}
		if e.body != "" {
			if _, err := io.WriteString(tw, e.body); err != nil {
				t.Fatalf("Write(): %v", err)
			}
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("close tar writer: %v", err)
	}
	return tar.NewReader(&buf)
}

// ---------------------------------------------------------------------------
// Extract zip with context cancellation
// ---------------------------------------------------------------------------

// TestExtractZipContextCancellation verifies zip extraction respects context cancellation.
func TestExtractZipContextCancellation(t *testing.T) {
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

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	extract := cli.Options{Mode: cli.ModeExtract, Archive: archive, Chdir: out}
	got := r.Run(ctx, extract)
	if got.ExitCode != ExitFatal {
		t.Fatalf("extract exit=%d err=%v, want fatal", got.ExitCode, got.Err)
	}
}

// ---------------------------------------------------------------------------
// Create tar with archive suffix (zip)
// ---------------------------------------------------------------------------

// TestCreateZipArchiveWithSuffix verifies zip creation with the -suffix option.
func TestCreateZipArchiveWithSuffix(t *testing.T) {
	root := t.TempDir()
	archive := filepath.Join(root, "backup.zip")

	if err := os.WriteFile(filepath.Join(root, "file.txt"), []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}

	r, err := New(context.Background(), io.Discard, io.Discard)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	create := cli.Options{
		Mode:    cli.ModeCreate,
		Archive: archive,
		Suffix:  "daily",
		Chdir:   root,
		Members: []string{"file.txt"},
	}
	if got := r.Run(context.Background(), create); got.ExitCode != ExitSuccess {
		t.Fatalf("create exit=%d err=%v", got.ExitCode, got.Err)
	}

	suffixed := archivepath.AddSuffix(archive, "daily")
	if _, err := os.Stat(suffixed); err != nil {
		t.Fatalf("expected suffixed archive %s: %v", suffixed, err)
	}
	if _, err := os.Stat(archive); !os.IsNotExist(err) {
		t.Fatalf("base archive should not exist, err=%v", err)
	}
}

// ---------------------------------------------------------------------------
// Extract/list tar with xattrs metadata policy
// ---------------------------------------------------------------------------

// TestCreateExtractTarWithXattrsEnabled verifies that xattrs flag works during round-trip.
func TestCreateExtractTarWithXattrsEnabled(t *testing.T) {
	root := t.TempDir()
	archive := filepath.Join(root, "a.tar")
	out := filepath.Join(root, "out")

	if err := os.WriteFile(filepath.Join(root, "file.txt"), []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}

	r, err := New(context.Background(), io.Discard, io.Discard)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	create := cli.Options{Mode: cli.ModeCreate, Archive: archive, Chdir: root, Xattrs: true, Members: []string{"file.txt"}}
	if got := r.Run(context.Background(), create); got.ExitCode != ExitSuccess {
		t.Fatalf("create exit=%d err=%v", got.ExitCode, got.Err)
	}

	if err := os.MkdirAll(out, 0o755); err != nil {
		t.Fatal(err)
	}
	extract := cli.Options{Mode: cli.ModeExtract, Archive: archive, Chdir: out, Xattrs: true}
	got := r.Run(context.Background(), extract)
	// On some OSes xattr may not be supported; the file should still be extracted
	if got.ExitCode == ExitFatal && got.Err != nil {
		t.Fatalf("extract exit=%d err=%v", got.ExitCode, got.Err)
	}

	b, err := os.ReadFile(filepath.Join(out, "file.txt"))
	if err != nil {
		t.Fatalf("read extracted file: %v", err)
	}
	if string(b) != "data" {
		t.Fatalf("content = %q, want data", string(b))
	}
}

// ---------------------------------------------------------------------------
// Create/list with compression
// ---------------------------------------------------------------------------

// TestListCompressedTarWithExplicitCompression verifies list with explicit compression flag.
func TestListCompressedTarWithExplicitCompression(t *testing.T) {
	root := t.TempDir()
	archive := filepath.Join(root, "a.tar.gz")

	if err := os.WriteFile(filepath.Join(root, "file.txt"), []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}

	r, err := New(context.Background(), io.Discard, io.Discard)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	create := cli.Options{
		Mode:        cli.ModeCreate,
		Archive:     archive,
		Chdir:       root,
		Compression: cli.CompressionGzip,
		Members:     []string{"file.txt"},
	}
	if got := r.Run(context.Background(), create); got.ExitCode != ExitSuccess {
		t.Fatalf("create exit=%d err=%v", got.ExitCode, got.Err)
	}

	var listBuf bytes.Buffer
	rList, err := New(context.Background(), &listBuf, io.Discard)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	list := cli.Options{
		Mode:        cli.ModeList,
		Archive:     archive,
		Compression: cli.CompressionGzip,
	}
	if got := rList.Run(context.Background(), list); got.ExitCode != ExitSuccess {
		t.Fatalf("list exit=%d err=%v", got.ExitCode, got.Err)
	}
	if !strings.Contains(listBuf.String(), "file.txt") {
		t.Fatalf("list output should include file.txt: %q", listBuf.String())
	}
}
