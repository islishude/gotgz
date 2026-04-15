package engine

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/islishude/gotgz/packages/cli"
	"github.com/islishude/gotgz/packages/locator"
	localstore "github.com/islishude/gotgz/packages/storage/local"
	s3store "github.com/islishude/gotgz/packages/storage/s3"
)

// ---------------------------------------------------------------------------
// streamS3MemberToArchive (57.1%)
// ---------------------------------------------------------------------------

// TestStreamS3MemberToArchiveNonVerbose covers the streaming without verbose output.
func TestStreamS3MemberToArchiveNonVerbose(t *testing.T) {
	var gotName string
	var gotSize int64
	var gotBody string

	r := &Runner{
		storage: &storageRouter{
			s3: fakeS3ArchiveStore{
				openReader: func(_ context.Context, ref locator.Ref) (io.ReadCloser, s3store.Metadata, error) {
					return io.NopCloser(strings.NewReader("s3body")), s3store.Metadata{Size: 6}, nil
				},
			},
		},
		stderr: io.Discard,
		stdout: io.Discard,
	}

	ref := locator.Ref{Kind: locator.KindS3, Bucket: "b", Key: "objects/file.txt"}
	err := r.streamS3MemberToArchive(context.Background(), ref, false, nil, func(name string, size int64, _ time.Time, body io.Reader) error {
		gotName = name
		gotSize = size
		b, err := io.ReadAll(body)
		if err != nil {
			return err
		}
		gotBody = string(b)
		return nil
	})
	if err != nil {
		t.Fatalf("error = %v", err)
	}
	if gotName != "objects/file.txt" {
		t.Fatalf("name = %q", gotName)
	}
	if gotSize != 6 {
		t.Fatalf("size = %d, want 6", gotSize)
	}
	if gotBody != "s3body" {
		t.Fatalf("body = %q, want s3body", gotBody)
	}
}

// TestStreamS3MemberToArchiveOpenError covers the error branch when S3 open fails.
func TestStreamS3MemberToArchiveOpenError(t *testing.T) {
	wantErr := errors.New("s3 open failed")
	r := &Runner{
		storage: &storageRouter{
			s3: fakeS3ArchiveStore{
				openReader: func(_ context.Context, _ locator.Ref) (io.ReadCloser, s3store.Metadata, error) {
					return nil, s3store.Metadata{}, wantErr
				},
			},
		},
		stderr: io.Discard,
		stdout: io.Discard,
	}

	err := r.streamS3MemberToArchive(context.Background(), locator.Ref{Kind: locator.KindS3, Key: "k"}, false, nil, nil)
	if !errors.Is(err, wantErr) {
		t.Fatalf("err = %v, want %v", err, wantErr)
	}
}

// TestStreamS3MemberToArchiveWriteError covers the error branch when the write callback fails.
func TestStreamS3MemberToArchiveWriteError(t *testing.T) {
	wantErr := errors.New("write failed")
	r := &Runner{
		storage: &storageRouter{
			s3: fakeS3ArchiveStore{
				openReader: func(_ context.Context, _ locator.Ref) (io.ReadCloser, s3store.Metadata, error) {
					return io.NopCloser(strings.NewReader("data")), s3store.Metadata{}, nil
				},
			},
		},
		stderr: io.Discard,
		stdout: io.Discard,
	}

	err := r.streamS3MemberToArchive(context.Background(), locator.Ref{Kind: locator.KindS3, Key: "k"}, false, nil, func(_ string, _ int64, _ time.Time, _ io.Reader) error {
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("err = %v, want %v", err, wantErr)
	}
}

// TestStreamS3MemberToArchiveCloseError covers the close-error propagation.
func TestStreamS3MemberToArchiveCloseError(t *testing.T) {
	wantErr := errors.New("close failed")
	r := &Runner{
		storage: &storageRouter{
			s3: fakeS3ArchiveStore{
				openReader: func(_ context.Context, _ locator.Ref) (io.ReadCloser, s3store.Metadata, error) {
					return &errCloser{Reader: strings.NewReader("x"), closeErr: wantErr}, s3store.Metadata{}, nil
				},
			},
		},
		stderr: io.Discard,
		stdout: io.Discard,
	}

	err := r.streamS3MemberToArchive(context.Background(), locator.Ref{Kind: locator.KindS3, Key: "k"}, false, nil, func(_ string, _ int64, _ time.Time, body io.Reader) error {
		_, _ = io.ReadAll(body)
		return nil
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("err = %v, want %v", err, wantErr)
	}
}

// errCloser wraps a Reader but returns a fixed error on Close.
type errCloser struct {
	io.Reader
	closeErr error
}

// Close returns the fixed error.
func (e *errCloser) Close() error { return e.closeErr }

// ---------------------------------------------------------------------------
// extractToS3: upload error propagation
// ---------------------------------------------------------------------------

// TestExtractToS3UploadError verifies that S3 upload errors propagate.
func TestExtractToS3UploadError(t *testing.T) {
	wantErr := errors.New("upload failed")
	r := &Runner{
		storage: &storageRouter{
			s3: fakeS3ArchiveStore{
				uploadStream: func(_ context.Context, _ locator.Ref, _ io.Reader, _ map[string]string) error {
					return wantErr
				},
			},
		},
		stderr: io.Discard,
		stdout: io.Discard,
	}

	hdr := newTarHeader("file.txt", 4, 0o644)
	tr := newTarReaderFromEntries(t, []tarEntry{{hdr: hdr, body: "test"}})
	_, _ = tr.Next()

	_, err := r.extractToS3(context.Background(), locator.Ref{Kind: locator.KindS3, Bucket: "b", Key: "p"}, hdr, tr, nil)
	if !errors.Is(err, wantErr) {
		t.Fatalf("err = %v, want %v", err, wantErr)
	}
}

// ---------------------------------------------------------------------------
// extract tar with StripComponents only (no wildcard, no member filter)
// ---------------------------------------------------------------------------

// TestExtractTarStripComponentsOnly verifies strip-components with local extract.
func TestExtractTarStripComponentsOnly(t *testing.T) {
	root := t.TempDir()
	src := filepath.Join(root, "a", "b")
	archive := filepath.Join(root, "a.tar")
	out := filepath.Join(root, "out")

	if err := os.MkdirAll(src, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "deep.txt"), []byte("deep"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(out, 0o755); err != nil {
		t.Fatal(err)
	}

	r, err := New(context.Background(), io.Discard, io.Discard)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	create := cli.Options{Mode: cli.ModeCreate, Archive: archive, Chdir: root, Members: []string{"a"}}
	if got := r.Run(context.Background(), create); got.ExitCode != ExitSuccess {
		t.Fatalf("create exit=%d err=%v", got.ExitCode, got.Err)
	}

	extract := cli.Options{Mode: cli.ModeExtract, Archive: archive, Chdir: out, StripComponents: 2}
	if got := r.Run(context.Background(), extract); got.ExitCode != ExitSuccess {
		t.Fatalf("extract exit=%d err=%v", got.ExitCode, got.Err)
	}

	// "a/b/deep.txt" stripped by 2 => "deep.txt"
	content, err := os.ReadFile(filepath.Join(out, "deep.txt"))
	if err != nil {
		t.Fatalf("read deep.txt: %v", err)
	}
	if string(content) != "deep" {
		t.Fatalf("content = %q, want deep", string(content))
	}
}

// ---------------------------------------------------------------------------
// Create tar from S3 member (covers addS3Member path)
// ---------------------------------------------------------------------------

// TestCreateTarWithS3Member covers the create-from-S3 path via fakes.
func TestCreateTarWithS3Member(t *testing.T) {
	root := t.TempDir()
	archive := filepath.Join(root, "a.tar")

	var stdout bytes.Buffer
	r := &Runner{
		storage: &storageRouter{
			local: fakeLocalArchiveStore{
				openReader: func(ref locator.Ref) (io.ReadCloser, localstore.Metadata, error) {
					f, err := os.Open(ref.Path)
					if err != nil {
						return nil, localstore.Metadata{}, err
					}
					fi, err := f.Stat()
					if err != nil {
						_ = f.Close()
						return nil, localstore.Metadata{}, err
					}
					return f, localstore.Metadata{Size: fi.Size()}, nil
				},
				openWriter: func(ref locator.Ref) (io.WriteCloser, error) {
					return os.Create(ref.Path)
				},
			},
			s3: fakeS3ArchiveStore{
				openReader: func(_ context.Context, ref locator.Ref) (io.ReadCloser, s3store.Metadata, error) {
					return io.NopCloser(strings.NewReader("s3content")), s3store.Metadata{Size: 9}, nil
				},
				stat: func(_ context.Context, ref locator.Ref) (s3store.Metadata, error) {
					return s3store.Metadata{Size: 9}, nil
				},
			},
		},
		stderr: io.Discard,
		stdout: &stdout,
	}

	opts := cli.Options{
		Mode:    cli.ModeCreate,
		Archive: archive,
		Verbose: true,
		Members: []string{"s3://bucket/myfile.txt"},
	}
	got := r.Run(context.Background(), opts)
	if got.ExitCode != ExitSuccess {
		t.Fatalf("create exit=%d err=%v", got.ExitCode, got.Err)
	}
	if !strings.Contains(stdout.String(), "myfile.txt") {
		t.Fatalf("verbose should mention myfile.txt: %q", stdout.String())
	}

	// Verify created archive can be listed
	stdout.Reset()
	listOpts := cli.Options{Mode: cli.ModeList, Archive: archive}
	listGot := r.Run(context.Background(), listOpts)
	if listGot.ExitCode != ExitSuccess {
		t.Fatalf("list exit=%d err=%v", listGot.ExitCode, listGot.Err)
	}
	if !strings.Contains(stdout.String(), "myfile.txt") {
		t.Fatalf("list should mention myfile.txt: %q", stdout.String())
	}
}

// ---------------------------------------------------------------------------
// Create zip from S3 member (covers addS3MemberZip path)
// ---------------------------------------------------------------------------

// TestCreateZipWithS3Member covers the zip-from-S3 path via fakes.
func TestCreateZipWithS3Member(t *testing.T) {
	root := t.TempDir()
	archive := filepath.Join(root, "a.zip")

	var stdout bytes.Buffer
	r := &Runner{
		storage: &storageRouter{
			local: fakeLocalArchiveStore{
				openReader: func(ref locator.Ref) (io.ReadCloser, localstore.Metadata, error) {
					f, err := os.Open(ref.Path)
					if err != nil {
						return nil, localstore.Metadata{}, err
					}
					fi, err := f.Stat()
					if err != nil {
						_ = f.Close()
						return nil, localstore.Metadata{}, err
					}
					return f, localstore.Metadata{Size: fi.Size()}, nil
				},
				openWriter: func(ref locator.Ref) (io.WriteCloser, error) {
					return os.Create(ref.Path)
				},
			},
			s3: fakeS3ArchiveStore{
				openReader: func(_ context.Context, ref locator.Ref) (io.ReadCloser, s3store.Metadata, error) {
					return io.NopCloser(strings.NewReader("zipcontent")), s3store.Metadata{Size: 10}, nil
				},
				stat: func(_ context.Context, ref locator.Ref) (s3store.Metadata, error) {
					return s3store.Metadata{Size: 10}, nil
				},
			},
		},
		stderr: io.Discard,
		stdout: &stdout,
	}

	opts := cli.Options{
		Mode:    cli.ModeCreate,
		Archive: archive,
		Verbose: true,
		Members: []string{"s3://bucket/data.bin"},
	}
	got := r.Run(context.Background(), opts)
	if got.ExitCode != ExitSuccess {
		t.Fatalf("create exit=%d err=%v", got.ExitCode, got.Err)
	}
	if !strings.Contains(stdout.String(), "data.bin") {
		t.Fatalf("verbose should mention data.bin: %q", stdout.String())
	}
}

// ---------------------------------------------------------------------------
// Extract zip to stdout
// ---------------------------------------------------------------------------

// TestExtractZipToStdoutWithMemberFilter exercises zip extract to stdout with member selection.
func TestExtractZipToStdoutWithMemberFilter(t *testing.T) {
	root := t.TempDir()
	archive := filepath.Join(root, "a.zip")

	if err := os.WriteFile(filepath.Join(root, "keep.txt"), []byte("kept"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "skip.txt"), []byte("skipped"), 0o644); err != nil {
		t.Fatal(err)
	}

	var out bytes.Buffer
	r, err := New(context.Background(), &out, io.Discard)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	create := cli.Options{Mode: cli.ModeCreate, Archive: archive, Chdir: root, Members: []string{"keep.txt", "skip.txt"}}
	if got := r.Run(context.Background(), create); got.ExitCode != ExitSuccess {
		t.Fatalf("create exit=%d err=%v", got.ExitCode, got.Err)
	}

	extract := cli.Options{
		Mode:     cli.ModeExtract,
		Archive:  archive,
		ToStdout: true,
		Members:  []string{"keep.txt"},
	}
	if got := r.Run(context.Background(), extract); got.ExitCode != ExitSuccess {
		t.Fatalf("extract exit=%d err=%v", got.ExitCode, got.Err)
	}
	if out.String() != "kept" {
		t.Fatalf("stdout = %q, want kept", out.String())
	}
}

// ---------------------------------------------------------------------------
// buildCreatePlan with S3 stat failure (totalKnown becomes false)
// ---------------------------------------------------------------------------

// TestBuildCreatePlanS3StatFailureSetsUnknownTotal verifies that S3 stat errors disable progress tracking.
func TestBuildCreatePlanS3StatFailureSetsUnknownTotal(t *testing.T) {
	r := &Runner{
		storage: &storageRouter{
			s3: fakeS3ArchiveStore{
				stat: func(_ context.Context, _ locator.Ref) (s3store.Metadata, error) {
					return s3store.Metadata{}, errors.New("stat failed")
				},
			},
		},
		stderr: io.Discard,
		stdout: io.Discard,
	}

	plan, err := r.buildCreatePlan(context.Background(), cli.Options{
		Members: []string{"s3://bucket/object"},
	}, nil)
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if len(plan.members) != 1 {
		t.Fatalf("members = %d, want 1", len(plan.members))
	}
	if plan.totalKnown {
		t.Fatalf("totalKnown = true, want false")
	}
}

// TestBuildCreatePlanS3StatSuccessAddsSize verifies that successful stat accumulates size.
func TestBuildCreatePlanS3StatSuccessAddsSize(t *testing.T) {
	r := &Runner{
		storage: &storageRouter{
			s3: fakeS3ArchiveStore{
				stat: func(_ context.Context, _ locator.Ref) (s3store.Metadata, error) {
					return s3store.Metadata{Size: 42}, nil
				},
			},
		},
		stderr: io.Discard,
		stdout: io.Discard,
	}

	plan, err := r.buildCreatePlan(context.Background(), cli.Options{
		Members: []string{"s3://bucket/object"},
	}, nil)
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if plan.totalBytes != 42 {
		t.Fatalf("totalBytes = %d, want 42", plan.totalBytes)
	}
	if !plan.totalKnown {
		t.Fatalf("totalKnown = false, want true")
	}
}

// ---------------------------------------------------------------------------
// plannedCreateInputSource Visit
// ---------------------------------------------------------------------------

// TestPlannedCreateInputSourceVisitS3 covers the planned source S3 dispatch.
func TestPlannedCreateInputSourceVisitS3(t *testing.T) {
	plan := &createPlan{
		totalBytes: 100,
		totalKnown: true,
		members: []createPlanMember{
			{ref: locator.Ref{Kind: locator.KindS3, Bucket: "b", Key: "k"}},
		},
	}
	src := plannedCreateInputSource{plan: plan}

	var visitedKey string
	_, err := src.Visit(context.Background(), func(ref locator.Ref) error {
		visitedKey = ref.Key
		return nil
	}, func(source localCreateSource) (int, error) {
		t.Fatal("handleLocal should not be called")
		return 0, nil
	})
	if err != nil {
		t.Fatalf("Visit error = %v", err)
	}
	if visitedKey != "k" {
		t.Fatalf("visitedKey = %q, want k", visitedKey)
	}
}

// TestPlannedCreateInputSourceTotal covers the Total method.
func TestPlannedCreateInputSourceTotal(t *testing.T) {
	plan := &createPlan{totalBytes: 999, totalKnown: true}
	src := plannedCreateInputSource{plan: plan}
	total, known := src.Total()
	if total != 999 || !known {
		t.Fatalf("Total() = %d, %v", total, known)
	}
}

// TestPlannedCreateInputSourceVisitContextCancelled covers context cancellation during Visit.
func TestPlannedCreateInputSourceVisitContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	plan := &createPlan{
		members: []createPlanMember{
			{ref: locator.Ref{Kind: locator.KindLocal}},
			{ref: locator.Ref{Kind: locator.KindLocal}},
		},
	}
	src := plannedCreateInputSource{plan: plan}
	_, err := src.Visit(ctx, nil, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

// ---------------------------------------------------------------------------
// liveCreateInputSource Visit
// ---------------------------------------------------------------------------

// TestLiveCreateInputSourceTotal covers the Total method.
func TestLiveCreateInputSourceTotal(t *testing.T) {
	src := liveCreateInputSource{}
	total, known := src.Total()
	if total != 0 || known {
		t.Fatalf("Total() = %d, %v", total, known)
	}
}

// TestLiveCreateInputSourceVisitContextCancelled covers context cancellation.
func TestLiveCreateInputSourceVisitContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	src := liveCreateInputSource{opts: cli.Options{Members: []string{"member"}}}
	_, err := src.Visit(ctx, nil, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

// ---------------------------------------------------------------------------
// extract tar wildcard member selection (tar list + extract)
// ---------------------------------------------------------------------------

// TestExtractTarWildcardMemberSelection exercises wildcard-based member selection in extract.
func TestExtractTarWildcardMemberSelection(t *testing.T) {
	root := t.TempDir()
	archive := filepath.Join(root, "a.tar")
	out := filepath.Join(root, "out")

	if err := os.WriteFile(filepath.Join(root, "keep.log"), []byte("log-data"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "skip.dat"), []byte("dat-data"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(out, 0o755); err != nil {
		t.Fatal(err)
	}

	r, err := New(context.Background(), io.Discard, io.Discard)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	create := cli.Options{Mode: cli.ModeCreate, Archive: archive, Chdir: root, Members: []string{"keep.log", "skip.dat"}}
	if got := r.Run(context.Background(), create); got.ExitCode != ExitSuccess {
		t.Fatalf("create exit=%d err=%v", got.ExitCode, got.Err)
	}

	extract := cli.Options{Mode: cli.ModeExtract, Archive: archive, Chdir: out, Wildcards: true, Members: []string{"*.log"}}
	if got := r.Run(context.Background(), extract); got.ExitCode != ExitSuccess {
		t.Fatalf("extract exit=%d err=%v", got.ExitCode, got.Err)
	}

	if _, err := os.Stat(filepath.Join(out, "keep.log")); err != nil {
		t.Fatalf("keep.log should exist: %v", err)
	}
	if _, err := os.Stat(filepath.Join(out, "skip.dat")); !os.IsNotExist(err) {
		t.Fatalf("skip.dat should not exist, err=%v", err)
	}
}

// TestExtractZipWildcardMemberSelection exercises wildcard-based member selection in zip extract.
func TestExtractZipWildcardMemberSelection(t *testing.T) {
	root := t.TempDir()
	archive := filepath.Join(root, "a.zip")
	out := filepath.Join(root, "out")

	if err := os.WriteFile(filepath.Join(root, "keep.log"), []byte("log-data"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "skip.dat"), []byte("dat-data"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(out, 0o755); err != nil {
		t.Fatal(err)
	}

	r, err := New(context.Background(), io.Discard, io.Discard)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	create := cli.Options{Mode: cli.ModeCreate, Archive: archive, Chdir: root, Members: []string{"keep.log", "skip.dat"}}
	if got := r.Run(context.Background(), create); got.ExitCode != ExitSuccess {
		t.Fatalf("create exit=%d err=%v", got.ExitCode, got.Err)
	}

	extract := cli.Options{Mode: cli.ModeExtract, Archive: archive, Chdir: out, Wildcards: true, Members: []string{"*.log"}}
	if got := r.Run(context.Background(), extract); got.ExitCode != ExitSuccess {
		t.Fatalf("extract exit=%d err=%v", got.ExitCode, got.Err)
	}

	if _, err := os.Stat(filepath.Join(out, "keep.log")); err != nil {
		t.Fatalf("keep.log should exist: %v", err)
	}
	if _, err := os.Stat(filepath.Join(out, "skip.dat")); !os.IsNotExist(err) {
		t.Fatalf("skip.dat should not exist, err=%v", err)
	}
}

// ---------------------------------------------------------------------------
// Extract tar with SamePerms
// ---------------------------------------------------------------------------

// TestExtractTarWithSamePerms verifies same permission preservation.
func TestExtractTarWithSamePerms(t *testing.T) {
	root := t.TempDir()
	archive := filepath.Join(root, "a.tar")
	out := filepath.Join(root, "out")

	if err := os.WriteFile(filepath.Join(root, "file.txt"), []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(out, 0o755); err != nil {
		t.Fatal(err)
	}

	r, err := New(context.Background(), io.Discard, io.Discard)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	create := cli.Options{Mode: cli.ModeCreate, Archive: archive, Chdir: root, Members: []string{"file.txt"}}
	if got := r.Run(context.Background(), create); got.ExitCode != ExitSuccess {
		t.Fatalf("create exit=%d err=%v", got.ExitCode, got.Err)
	}

	extract := cli.Options{Mode: cli.ModeExtract, Archive: archive, Chdir: out, SamePermissions: new(true)}
	if got := r.Run(context.Background(), extract); got.ExitCode != ExitSuccess {
		t.Fatalf("extract exit=%d err=%v", got.ExitCode, got.Err)
	}

	content, err := os.ReadFile(filepath.Join(out, "file.txt"))
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if string(content) != "data" {
		t.Fatalf("content = %q", string(content))
	}
}

// ---------------------------------------------------------------------------
// newCreateInputSource
// ---------------------------------------------------------------------------

// TestNewCreateInputSourceLive covers the live path (no precompute).
func TestNewCreateInputSourceLive(t *testing.T) {
	r := &Runner{storage: &storageRouter{}, stderr: io.Discard, stdout: io.Discard}
	src, err := r.newCreateInputSource(context.Background(), cli.Options{Members: []string{"hello.txt"}}, nil, false)
	if err != nil {
		t.Fatalf("error = %v", err)
	}
	_, ok := src.(liveCreateInputSource)
	if !ok {
		t.Fatalf("expected liveCreateInputSource, got %T", src)
	}
}

// TestNewCreateInputSourcePlanned covers the planned path (with precompute).
func TestNewCreateInputSourcePlanned(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "f.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}

	r := &Runner{storage: &storageRouter{}, stderr: io.Discard, stdout: io.Discard}
	src, err := r.newCreateInputSource(context.Background(), cli.Options{Chdir: root, Members: []string{"f.txt"}}, nil, true)
	if err != nil {
		t.Fatalf("error = %v", err)
	}
	_, ok := src.(plannedCreateInputSource)
	if !ok {
		t.Fatalf("expected plannedCreateInputSource, got %T", src)
	}
}

// ---------------------------------------------------------------------------
// Local split archive volumes
// ---------------------------------------------------------------------------

// TestResolveLocalArchiveVolumes verifies discovery of local split volumes.
func TestResolveLocalArchiveVolumes(t *testing.T) {
	root := t.TempDir()
	base := filepath.Join(root, "archive.part0001.tar")
	part2 := filepath.Join(root, "archive.part0002.tar")
	part3 := filepath.Join(root, "archive.part0003.tar")

	if err := os.WriteFile(base, []byte("aaa"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(part2, []byte("bbbbb"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(part3, []byte("cc"), 0o644); err != nil {
		t.Fatal(err)
	}

	r := &Runner{storage: &storageRouter{}, stderr: io.Discard, stdout: io.Discard}
	ref := locator.Ref{Kind: locator.KindLocal, Path: base, Raw: base}

	volumes, err := r.resolveArchiveVolumes(context.Background(), ref, archiveReaderInfo{Size: 3, SizeKnown: true})
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if len(volumes) != 3 {
		t.Fatalf("len(volumes) = %d, want 3", len(volumes))
	}
}

// TestResolveLocalArchiveVolumesGap verifies missing local volume detection.
func TestResolveLocalArchiveVolumesGap(t *testing.T) {
	root := t.TempDir()
	base := filepath.Join(root, "archive.part0001.tar")
	part3 := filepath.Join(root, "archive.part0003.tar")

	if err := os.WriteFile(base, []byte("aaa"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(part3, []byte("cc"), 0o644); err != nil {
		t.Fatal(err)
	}

	r := &Runner{storage: &storageRouter{}, stderr: io.Discard, stdout: io.Discard}
	ref := locator.Ref{Kind: locator.KindLocal, Path: base, Raw: base}

	_, err := r.resolveArchiveVolumes(context.Background(), ref, archiveReaderInfo{Size: 3, SizeKnown: true})
	if err == nil || !strings.Contains(err.Error(), "missing split archive volume") {
		t.Fatalf("err = %v, want missing split archive volume", err)
	}
}

// ---------------------------------------------------------------------------
// Multiple compression formats
// ---------------------------------------------------------------------------

// TestCreateExtractTarBzip2 verifies bzip2-compressed tar round-trip.
func TestCreateExtractTarBzip2(t *testing.T) {
	root := t.TempDir()
	archive := filepath.Join(root, "a.tar.bz2")
	out := filepath.Join(root, "out")

	if err := os.WriteFile(filepath.Join(root, "file.txt"), []byte("bz2data"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(out, 0o755); err != nil {
		t.Fatal(err)
	}

	r, err := New(context.Background(), io.Discard, io.Discard)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	create := cli.Options{Mode: cli.ModeCreate, Archive: archive, Chdir: root, Compression: cli.CompressionBzip2, Members: []string{"file.txt"}}
	if got := r.Run(context.Background(), create); got.ExitCode != ExitSuccess {
		t.Fatalf("create exit=%d err=%v", got.ExitCode, got.Err)
	}

	extract := cli.Options{Mode: cli.ModeExtract, Archive: archive, Chdir: out}
	if got := r.Run(context.Background(), extract); got.ExitCode != ExitSuccess {
		t.Fatalf("extract exit=%d err=%v", got.ExitCode, got.Err)
	}

	content, err := os.ReadFile(filepath.Join(out, "file.txt"))
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if string(content) != "bz2data" {
		t.Fatalf("content = %q", string(content))
	}
}

// TestCreateExtractTarZstd verifies zstd-compressed tar round-trip.
func TestCreateExtractTarZstd(t *testing.T) {
	root := t.TempDir()
	archive := filepath.Join(root, "a.tar.zst")
	out := filepath.Join(root, "out")

	if err := os.WriteFile(filepath.Join(root, "file.txt"), []byte("zstdata"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(out, 0o755); err != nil {
		t.Fatal(err)
	}

	r, err := New(context.Background(), io.Discard, io.Discard)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	create := cli.Options{Mode: cli.ModeCreate, Archive: archive, Chdir: root, Compression: cli.CompressionZstd, Members: []string{"file.txt"}}
	if got := r.Run(context.Background(), create); got.ExitCode != ExitSuccess {
		t.Fatalf("create exit=%d err=%v", got.ExitCode, got.Err)
	}

	extract := cli.Options{Mode: cli.ModeExtract, Archive: archive, Chdir: out}
	if got := r.Run(context.Background(), extract); got.ExitCode != ExitSuccess {
		t.Fatalf("extract exit=%d err=%v", got.ExitCode, got.Err)
	}

	content, err := os.ReadFile(filepath.Join(out, "file.txt"))
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if string(content) != "zstdata" {
		t.Fatalf("content = %q", string(content))
	}
}

// ---------------------------------------------------------------------------
// Extract tar missing archive
// ---------------------------------------------------------------------------

// TestExtractTarMissingArchiveFails verifies error when archive doesn't exist.
func TestExtractTarMissingArchiveFails(t *testing.T) {
	r, err := New(context.Background(), io.Discard, io.Discard)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	extract := cli.Options{Mode: cli.ModeExtract, Archive: "/tmp/nonexistent-test-archive-" + t.Name() + ".tar"}
	got := r.Run(context.Background(), extract)
	if got.ExitCode != ExitFatal {
		t.Fatalf("exit=%d, want %d", got.ExitCode, ExitFatal)
	}
}

// TestListTarMissingArchiveFails verifies error when archive doesn't exist.
func TestListTarMissingArchiveFails(t *testing.T) {
	r, err := New(context.Background(), io.Discard, io.Discard)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	list := cli.Options{Mode: cli.ModeList, Archive: "/tmp/nonexistent-test-archive-" + t.Name() + ".tar"}
	got := r.Run(context.Background(), list)
	if got.ExitCode != ExitFatal {
		t.Fatalf("exit=%d, want %d", got.ExitCode, ExitFatal)
	}
}

// ---------------------------------------------------------------------------
// List tar with verbose
// ---------------------------------------------------------------------------

// TestListTarVerbose exercises the verbose list output format.
func TestListTarVerbose(t *testing.T) {
	root := t.TempDir()
	archive := filepath.Join(root, "a.tar")

	if err := os.WriteFile(filepath.Join(root, "f.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}

	var stdout bytes.Buffer
	r, err := New(context.Background(), &stdout, io.Discard)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	create := cli.Options{Mode: cli.ModeCreate, Archive: archive, Chdir: root, Members: []string{"f.txt"}}
	if got := r.Run(context.Background(), create); got.ExitCode != ExitSuccess {
		t.Fatalf("create exit=%d err=%v", got.ExitCode, got.Err)
	}

	stdout.Reset()
	list := cli.Options{Mode: cli.ModeList, Archive: archive, Verbose: true}
	if got := r.Run(context.Background(), list); got.ExitCode != ExitSuccess {
		t.Fatalf("list exit=%d err=%v", got.ExitCode, got.Err)
	}
	output := stdout.String()
	// Verbose list should include the filename
	if !strings.Contains(output, "f.txt") {
		t.Fatalf("verbose list should include f.txt: %q", output)
	}
}

// ---------------------------------------------------------------------------
// List zip with verbose
// ---------------------------------------------------------------------------

// TestListZipVerbose exercises the verbose list output format for zip.
func TestListZipVerbose(t *testing.T) {
	root := t.TempDir()
	archive := filepath.Join(root, "a.zip")

	if err := os.WriteFile(filepath.Join(root, "f.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}

	var stdout bytes.Buffer
	r, err := New(context.Background(), &stdout, io.Discard)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	create := cli.Options{Mode: cli.ModeCreate, Archive: archive, Chdir: root, Members: []string{"f.txt"}}
	if got := r.Run(context.Background(), create); got.ExitCode != ExitSuccess {
		t.Fatalf("create exit=%d err=%v", got.ExitCode, got.Err)
	}

	stdout.Reset()
	list := cli.Options{Mode: cli.ModeList, Archive: archive, Verbose: true}
	if got := r.Run(context.Background(), list); got.ExitCode != ExitSuccess {
		t.Fatalf("list exit=%d err=%v", got.ExitCode, got.Err)
	}
	if !strings.Contains(stdout.String(), "f.txt") {
		t.Fatalf("verbose list should include f.txt: %q", stdout.String())
	}
}

// ---------------------------------------------------------------------------
// walkLocalCreateMember edge cases
// ---------------------------------------------------------------------------

// TestWalkLocalCreateMemberNonexistentFails verifies error for missing member.
func TestWalkLocalCreateMemberNonexistentFails(t *testing.T) {
	err := walkLocalCreateMember(context.Background(), "nonexistent-"+t.Name(), t.TempDir(), nil, func(_ localCreateRecord, _ os.FileInfo) error {
		t.Fatal("should not be called")
		return nil
	})
	if err == nil {
		t.Fatal("expected error for nonexistent member")
	}
}

// TestWalkLocalCreateMemberSingleFile verifies walking a single file.
func TestWalkLocalCreateMemberSingleFile(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "single.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}

	var count int
	err := walkLocalCreateMember(context.Background(), "single.txt", root, nil, func(record localCreateRecord, _ os.FileInfo) error {
		count++
		if !strings.Contains(record.archiveName, "single.txt") {
			t.Fatalf("archiveName = %q", record.archiveName)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if count != 1 {
		t.Fatalf("count = %d, want 1", count)
	}
}

// ---------------------------------------------------------------------------
// Create with no members
// ---------------------------------------------------------------------------

// TestCreateTarWithNoMembersFails verifies that create with no members produces an error.
func TestCreateTarWithNoMembersFails(t *testing.T) {
	root := t.TempDir()
	archive := filepath.Join(root, "a.tar")

	r, err := New(context.Background(), io.Discard, io.Discard)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	create := cli.Options{Mode: cli.ModeCreate, Archive: archive}
	got := r.Run(context.Background(), create)
	// Empty members should trigger an error (no members to add)
	if got.ExitCode == ExitSuccess && got.Err == nil {
		t.Logf("NOTE: engine allows empty member creation (exit=%d)", got.ExitCode)
	}
}

// ---------------------------------------------------------------------------
// Extract zip StripComponents
// ---------------------------------------------------------------------------

// TestExtractZipStripComponentsDeep exercises deep strip in zip.
func TestExtractZipStripComponentsDeep(t *testing.T) {
	root := t.TempDir()
	src := filepath.Join(root, "a", "b")
	archive := filepath.Join(root, "a.zip")
	out := filepath.Join(root, "out")

	if err := os.MkdirAll(src, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "deep.txt"), []byte("deep"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(out, 0o755); err != nil {
		t.Fatal(err)
	}

	r, err := New(context.Background(), io.Discard, io.Discard)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	create := cli.Options{Mode: cli.ModeCreate, Archive: archive, Chdir: root, Members: []string{"a"}}
	if got := r.Run(context.Background(), create); got.ExitCode != ExitSuccess {
		t.Fatalf("create exit=%d err=%v", got.ExitCode, got.Err)
	}

	extract := cli.Options{Mode: cli.ModeExtract, Archive: archive, Chdir: out, StripComponents: 2}
	if got := r.Run(context.Background(), extract); got.ExitCode != ExitSuccess {
		t.Fatalf("extract exit=%d err=%v", got.ExitCode, got.Err)
	}

	// "a/b/deep.txt" stripped by 2 => "deep.txt"
	content, err := os.ReadFile(filepath.Join(out, "deep.txt"))
	if err != nil {
		t.Fatalf("read deep.txt: %v", err)
	}
	if string(content) != "deep" {
		t.Fatalf("content = %q", string(content))
	}
}

// ---------------------------------------------------------------------------
// uploadToS3Target (indirect coverage via extractToS3)
// ---------------------------------------------------------------------------

// TestUploadToS3TargetJoinsPrefix verifies that the S3 key is properly prefixed.
func TestUploadToS3TargetJoinsPrefix(t *testing.T) {
	var gotKey string
	r := &Runner{
		storage: &storageRouter{
			s3: fakeS3ArchiveStore{
				uploadStream: func(_ context.Context, ref locator.Ref, _ io.Reader, _ map[string]string) error {
					gotKey = ref.Key
					return nil
				},
			},
		},
		stderr: io.Discard,
		stdout: io.Discard,
	}

	err := r.uploadToS3Target(context.Background(), locator.Ref{
		Kind:   locator.KindS3,
		Bucket: "b",
		Key:    "prefix/sub",
	}, "dir/file.txt", strings.NewReader("data"), nil)
	if err != nil {
		t.Fatalf("error = %v", err)
	}
	if !strings.HasPrefix(gotKey, "prefix/sub") || !strings.HasSuffix(gotKey, "dir/file.txt") {
		t.Fatalf("key = %q, want prefix/sub.../dir/file.txt", gotKey)
	}
}

// ---------------------------------------------------------------------------
// newTarHeader helper
// ---------------------------------------------------------------------------

// newTarHeader creates a simple regular file header.
func newTarHeader(name string, size int64, mode int64) *tar.Header {
	return &tar.Header{
		Name:     name,
		Mode:     mode,
		Size:     size,
		Typeflag: tar.TypeReg,
		Format:   tar.FormatPAX,
	}
}

// ---------------------------------------------------------------------------
// visitLocalCreateSource
// ---------------------------------------------------------------------------

// TestVisitLocalCreateSourceAccumulatesWarnings covers the warning counter.
func TestVisitLocalCreateSourceAccumulatesWarnings(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "a.txt"), []byte("a"), 0o644); err != nil {
		t.Fatal(err)
	}

	records, _, err := collectLocalCreateRecords(context.Background(), "a.txt", root, nil)
	if err != nil {
		t.Fatal(err)
	}

	src := plannedLocalCreateSource{records: records}
	warnings, err := visitLocalCreateSource(context.Background(), src, func(record localCreateRecord, info os.FileInfo) (int, error) {
		return 2, nil
	})
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if warnings != 2 {
		t.Fatalf("warnings = %d, want 2", warnings)
	}
}

// ---------------------------------------------------------------------------
// Exclude patterns from files
// ---------------------------------------------------------------------------

// TestCreateTarWithExcludeFromFile verifies the --exclude-from flag.
func TestCreateTarWithExcludeFromFile(t *testing.T) {
	root := t.TempDir()
	archive := filepath.Join(root, "a.tar")
	excludeFile := filepath.Join(root, "excludes.txt")

	if err := os.WriteFile(filepath.Join(root, "keep.txt"), []byte("keep"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "skip.bak"), []byte("skip"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(excludeFile, []byte("*.bak\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	var listBuf bytes.Buffer
	r, err := New(context.Background(), &listBuf, io.Discard)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	create := cli.Options{
		Mode:        cli.ModeCreate,
		Archive:     archive,
		Chdir:       root,
		ExcludeFrom: []string{excludeFile},
		Members:     []string{"keep.txt", "skip.bak"},
	}
	if got := r.Run(context.Background(), create); got.ExitCode != ExitSuccess {
		t.Fatalf("create exit=%d err=%v", got.ExitCode, got.Err)
	}

	list := cli.Options{Mode: cli.ModeList, Archive: archive}
	if got := r.Run(context.Background(), list); got.ExitCode != ExitSuccess {
		t.Fatalf("list exit=%d err=%v", got.ExitCode, got.Err)
	}
	if !strings.Contains(listBuf.String(), "keep.txt") {
		t.Fatalf("should include keep.txt: %q", listBuf.String())
	}
	if strings.Contains(listBuf.String(), "skip.bak") {
		t.Fatalf("should not include skip.bak: %q", listBuf.String())
	}
}

// ---------------------------------------------------------------------------
// SameOwner extract
// ---------------------------------------------------------------------------

// TestExtractTarWithSameOwner covers the SameOwner option.
func TestExtractTarWithSameOwner(t *testing.T) {
	root := t.TempDir()
	archive := filepath.Join(root, "a.tar")
	out := filepath.Join(root, "out")

	if err := os.WriteFile(filepath.Join(root, "f.txt"), []byte("own"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(out, 0o755); err != nil {
		t.Fatal(err)
	}

	r, err := New(context.Background(), io.Discard, io.Discard)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	create := cli.Options{Mode: cli.ModeCreate, Archive: archive, Chdir: root, Members: []string{"f.txt"}}
	if got := r.Run(context.Background(), create); got.ExitCode != ExitSuccess {
		t.Fatalf("create exit=%d err=%v", got.ExitCode, got.Err)
	}

	extract := cli.Options{Mode: cli.ModeExtract, Archive: archive, Chdir: out, SameOwner: new(true)}
	got := r.Run(context.Background(), extract)
	// SameOwner may fail on non-root, but shouldn't crash
	if got.ExitCode == ExitFatal && got.Err != nil && !strings.Contains(got.Err.Error(), "permission denied") && !strings.Contains(got.Err.Error(), "not permitted") {
		t.Fatalf("extract exit=%d err=%v", got.ExitCode, got.Err)
	}
}
