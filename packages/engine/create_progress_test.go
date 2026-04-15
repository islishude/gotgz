package engine

import (
	"context"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/islishude/gotgz/packages/archiveprogress"
	"github.com/islishude/gotgz/packages/cli"
	"github.com/islishude/gotgz/packages/locator"
	"github.com/islishude/gotgz/packages/storage/s3"
)

// runCreateWithProgressMode executes one create-mode writer and returns how
// many times the fake S3 store's Stat path was used for progress estimation.
func runCreateWithProgressMode(t *testing.T, progress cli.ProgressMode, run func(*Runner, context.Context, cli.Options, locator.Ref, *archiveprogress.Reporter) (int, error), archiveRef locator.Ref) int {
	t.Helper()

	statCalls := 0
	openReaderCalls := 0
	runner := newRunner(
		fakeLocalArchiveStore{
			openWriter: func(ref locator.Ref) (io.WriteCloser, error) {
				if ref.Path != archiveRef.Path {
					t.Fatalf("archive path = %q, want %q", ref.Path, archiveRef.Path)
				}
				return &fakeWriteCloser{}, nil
			},
		},
		fakeS3ArchiveStore{
			stat: func(_ context.Context, ref locator.Ref) (s3.Metadata, error) {
				statCalls++
				if ref.Key != "object.txt" {
					t.Fatalf("stat key = %q, want %q", ref.Key, "object.txt")
				}
				return s3.Metadata{Size: int64(len("payload"))}, nil
			},
			openReader: func(_ context.Context, ref locator.Ref) (io.ReadCloser, s3.Metadata, error) {
				openReaderCalls++
				if ref.Key != "object.txt" {
					t.Fatalf("openReader key = %q, want %q", ref.Key, "object.txt")
				}
				return io.NopCloser(strings.NewReader("payload")), s3.Metadata{Size: int64(len("payload"))}, nil
			},
		},
		nil,
		io.Discard,
		io.Discard,
	)

	opts := cli.Options{
		Mode:     cli.ModeCreate,
		Archive:  archiveRef.Path,
		Progress: progress,
		Members:  []string{"s3://bucket/object.txt"},
	}
	reporter := archiveprogress.NewReporter(io.Discard, progress, 0, false, time.Now(), false)
	warnings, err := run(runner, context.Background(), opts, archiveRef, reporter)
	reporter.Finish()
	if err != nil {
		t.Fatalf("create error = %v", err)
	}
	if warnings != 0 {
		t.Fatalf("warnings = %d, want 0", warnings)
	}
	if openReaderCalls != 1 {
		t.Fatalf("openReaderCalls = %d, want 1", openReaderCalls)
	}
	return statCalls
}

// TestCreateModeSkipsS3SizeEstimationWhenProgressDisabled verifies that create
// mode does not pre-scan remote members when progress output is off.
func TestCreateModeSkipsS3SizeEstimationWhenProgressDisabled(t *testing.T) {
	cases := []struct {
		name       string
		archiveRef locator.Ref
		run        func(*Runner, context.Context, cli.Options, locator.Ref, *archiveprogress.Reporter) (int, error)
	}{
		{
			name:       "tar",
			archiveRef: locator.Ref{Kind: locator.KindLocal, Raw: "out.tar", Path: "out.tar"},
			run:        (*Runner).runCreateTar,
		},
		{
			name:       "zip",
			archiveRef: locator.Ref{Kind: locator.KindLocal, Raw: "out.zip", Path: "out.zip"},
			run:        (*Runner).runCreateZip,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if statCalls := runCreateWithProgressMode(t, cli.ProgressNever, tc.run, tc.archiveRef); statCalls != 0 {
				t.Fatalf("statCalls = %d, want 0", statCalls)
			}
		})
	}
}

// TestCreateModeEstimatesS3SizeWhenProgressEnabled verifies that create mode
// still preserves known-total progress when progress output is active.
func TestCreateModeEstimatesS3SizeWhenProgressEnabled(t *testing.T) {
	cases := []struct {
		name       string
		archiveRef locator.Ref
		run        func(*Runner, context.Context, cli.Options, locator.Ref, *archiveprogress.Reporter) (int, error)
	}{
		{
			name:       "tar",
			archiveRef: locator.Ref{Kind: locator.KindLocal, Raw: "out.tar", Path: "out.tar"},
			run:        (*Runner).runCreateTar,
		},
		{
			name:       "zip",
			archiveRef: locator.Ref{Kind: locator.KindLocal, Raw: "out.zip", Path: "out.zip"},
			run:        (*Runner).runCreateZip,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if statCalls := runCreateWithProgressMode(t, cli.ProgressAlways, tc.run, tc.archiveRef); statCalls != 1 {
				t.Fatalf("statCalls = %d, want 1", statCalls)
			}
		})
	}
}

// TestCreateModeValidatesInputsBeforeOpeningArchiveWriter verifies that create
// mode fails early on invalid exclude configuration before touching the output.
func TestCreateModeValidatesInputsBeforeOpeningArchiveWriter(t *testing.T) {
	cases := []struct {
		name       string
		archiveRef locator.Ref
		run        func(*Runner, context.Context, cli.Options, locator.Ref, *archiveprogress.Reporter) (int, error)
	}{
		{
			name:       "tar",
			archiveRef: locator.Ref{Kind: locator.KindLocal, Raw: "out.tar", Path: "out.tar"},
			run:        (*Runner).runCreateTar,
		},
		{
			name:       "zip",
			archiveRef: locator.Ref{Kind: locator.KindLocal, Raw: "out.zip", Path: "out.zip"},
			run:        (*Runner).runCreateZip,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			openWriterCalls := 0
			runner := newRunner(
				fakeLocalArchiveStore{
					openWriter: func(ref locator.Ref) (io.WriteCloser, error) {
						openWriterCalls++
						if ref.Path != tc.archiveRef.Path {
							t.Fatalf("archive path = %q, want %q", ref.Path, tc.archiveRef.Path)
						}
						return &fakeWriteCloser{}, nil
					},
				},
				nil,
				nil,
				io.Discard,
				io.Discard,
			)

			opts := cli.Options{
				Mode:        cli.ModeCreate,
				Archive:     tc.archiveRef.Path,
				ExcludeFrom: []string{filepath.Join(t.TempDir(), "missing-exclude.txt")},
			}
			reporter := archiveprogress.NewReporter(io.Discard, cli.ProgressNever, 0, false, time.Now(), false)
			warnings, err := tc.run(runner, context.Background(), opts, tc.archiveRef, reporter)
			reporter.Finish()

			if err == nil {
				t.Fatalf("create error = nil, want invalid exclude-from error")
			}
			if warnings != 0 {
				t.Fatalf("warnings = %d, want 0", warnings)
			}
			if openWriterCalls != 0 {
				t.Fatalf("openWriterCalls = %d, want 0", openWriterCalls)
			}
		})
	}
}
