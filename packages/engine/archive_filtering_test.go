package engine

import (
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

func TestCreateArchivesRespectExcludePatterns(t *testing.T) {
	tests := []struct {
		name    string
		archive string
	}{
		{name: "tar", archive: "bundle.tar"},
		{name: "zip", archive: "bundle.zip"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()
			writeFixtureTree(t, root, []fixtureEntry{{path: "src/keep.txt", body: "keep"}, {path: "src/nested/skip.log", body: "skip"}})
			archivePath := filepath.Join(root, tt.archive)
			r := newRunner(&localstore.ArchiveStore{}, nil, httpstore.New(), io.Discard, io.Discard)

			create := cli.Options{Mode: cli.ModeCreate, Archive: archivePath, Chdir: root, Members: []string{"src"}, Exclude: []string{"*.log"}}
			if got := r.Run(context.Background(), create); got.ExitCode != ExitSuccess {
				t.Fatalf("create exit=%d err=%v", got.ExitCode, got.Err)
			}

			var stdout bytes.Buffer
			listRunner := newRunner(&localstore.ArchiveStore{}, nil, httpstore.New(), &stdout, io.Discard)
			if got := listRunner.Run(context.Background(), cli.Options{Mode: cli.ModeList, Archive: archivePath}); got.ExitCode != ExitSuccess {
				t.Fatalf("list exit=%d err=%v", got.ExitCode, got.Err)
			}
			if strings.Contains(stdout.String(), "skip.log") {
				t.Fatalf("excluded file listed in archive:\n%s", stdout.String())
			}
			if !strings.Contains(stdout.String(), "keep.txt") {
				t.Fatalf("kept file missing from archive:\n%s", stdout.String())
			}
		})
	}
}

func TestListAndExtractApplyDirectoryMembersAndExcludes(t *testing.T) {
	root := t.TempDir()
	archivePath := filepath.Join(root, "filter.tar")
	outDir := filepath.Join(root, "out")
	if err := os.WriteFile(archivePath, tarArchiveBytes(t, map[string]string{
		"dir/keep.txt":        "keep",
		"dir/nested/skip.log": "skip",
		"other/file.txt":      "other",
	}), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	var stdout bytes.Buffer
	listRunner := newRunner(&localstore.ArchiveStore{}, nil, httpstore.New(), &stdout, io.Discard)
	listResult := listRunner.Run(context.Background(), cli.Options{
		Mode: cli.ModeList, Archive: archivePath, Members: []string{"dir"}, Exclude: []string{"*.log"},
	})
	if listResult.ExitCode != ExitSuccess {
		t.Fatalf("list result = %+v", listResult)
	}
	if got := stdout.String(); !strings.Contains(got, "dir/keep.txt") || strings.Contains(got, "skip.log") || strings.Contains(got, "other/") {
		t.Fatalf("list output = %q", got)
	}

	extractRunner := newRunner(&localstore.ArchiveStore{}, nil, httpstore.New(), io.Discard, io.Discard)
	extractResult := extractRunner.Run(context.Background(), cli.Options{
		Mode: cli.ModeExtract, Archive: archivePath, Chdir: outDir, Members: []string{"dir"}, Exclude: []string{"*.log"},
	})
	if extractResult.ExitCode != ExitSuccess {
		t.Fatalf("extract result = %+v", extractResult)
	}
	got, err := os.ReadFile(filepath.Join(outDir, "dir", "keep.txt"))
	if err != nil || string(got) != "keep" {
		t.Fatalf("keep.txt = %q, err=%v", got, err)
	}
	if _, err := os.Stat(filepath.Join(outDir, "dir", "nested", "skip.log")); !os.IsNotExist(err) {
		t.Fatalf("excluded skip.log stat err = %v", err)
	}
}

func TestSingleSegmentExcludeIsConsistentAcrossCreateListAndExtract(t *testing.T) {
	for _, suffix := range []string{".tar", ".zip"} {
		t.Run(suffix, func(t *testing.T) {
			root := t.TempDir()
			writeFixtureTree(t, root, []fixtureEntry{
				{path: "src/direct.txt", body: "direct"},
				{path: "src/cache/nested.txt", body: "nested"},
			})
			exclude := []string{"src/*"}
			filteredArchive := filepath.Join(root, "filtered"+suffix)
			createRunner := newRunner(&localstore.ArchiveStore{}, nil, httpstore.New(), io.Discard, io.Discard)
			if result := createRunner.Run(context.Background(), cli.Options{
				Mode: cli.ModeCreate, Archive: filteredArchive, Chdir: root, Members: []string{"src"}, Exclude: exclude,
			}); result.ExitCode != ExitSuccess {
				t.Fatalf("filtered create = %+v", result)
			}
			assertFilteredArchiveList(t, filteredArchive, nil)

			fullArchive := filepath.Join(root, "full"+suffix)
			if result := createRunner.Run(context.Background(), cli.Options{
				Mode: cli.ModeCreate, Archive: fullArchive, Chdir: root, Members: []string{"src"},
			}); result.ExitCode != ExitSuccess {
				t.Fatalf("full create = %+v", result)
			}
			assertFilteredArchiveList(t, fullArchive, exclude)

			outDir := filepath.Join(root, "out")
			extractRunner := newRunner(&localstore.ArchiveStore{}, nil, httpstore.New(), io.Discard, io.Discard)
			if result := extractRunner.Run(context.Background(), cli.Options{
				Mode: cli.ModeExtract, Archive: fullArchive, Chdir: outDir, Exclude: exclude,
			}); result.ExitCode != ExitSuccess {
				t.Fatalf("extract = %+v", result)
			}
			if body, err := os.ReadFile(filepath.Join(outDir, "src", "cache", "nested.txt")); err != nil || string(body) != "nested" {
				t.Fatalf("nested file = %q, err=%v", body, err)
			}
			if _, err := os.Stat(filepath.Join(outDir, "src", "direct.txt")); !os.IsNotExist(err) {
				t.Fatalf("direct file should be excluded, stat err=%v", err)
			}
		})
	}
}

func assertFilteredArchiveList(t *testing.T, archivePath string, exclude []string) {
	t.Helper()
	var stdout bytes.Buffer
	runner := newRunner(&localstore.ArchiveStore{}, nil, httpstore.New(), &stdout, io.Discard)
	if result := runner.Run(context.Background(), cli.Options{Mode: cli.ModeList, Archive: archivePath, Exclude: exclude}); result.ExitCode != ExitSuccess {
		t.Fatalf("list = %+v", result)
	}
	if got := stdout.String(); strings.Contains(got, "direct.txt") || !strings.Contains(got, "cache/nested.txt") {
		t.Fatalf("list output = %q, want nested file without direct file", got)
	}
}

func TestListTarWithMemberFilter(t *testing.T) {
	archivePath := filepath.Join(t.TempDir(), "filter.tar")
	if err := os.WriteFile(archivePath, tarArchiveBytes(t, map[string]string{"dir/keep.txt": "keep", "dir/skip.txt": "skip"}), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	var stdout bytes.Buffer
	r := newRunner(&localstore.ArchiveStore{}, nil, httpstore.New(), &stdout, io.Discard)
	if got := r.Run(context.Background(), cli.Options{Mode: cli.ModeList, Archive: archivePath, Members: []string{"dir/keep.txt"}}); got.ExitCode != ExitSuccess {
		t.Fatalf("list exit=%d err=%v", got.ExitCode, got.Err)
	}
	if strings.Contains(stdout.String(), "skip.txt") || !strings.Contains(stdout.String(), "keep.txt") {
		t.Fatalf("unexpected list output:\n%s", stdout.String())
	}
}

func TestExtractZipMemberSelection(t *testing.T) {
	root := t.TempDir()
	archivePath := filepath.Join(root, "filter.zip")
	outDir := filepath.Join(root, "out")
	if err := os.WriteFile(archivePath, zipArchiveBytes(t, map[string]string{"dir/keep.txt": "keep", "dir/skip.txt": "skip"}), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}

	r := newRunner(&localstore.ArchiveStore{}, nil, httpstore.New(), io.Discard, io.Discard)
	if got := r.Run(context.Background(), cli.Options{Mode: cli.ModeExtract, Archive: archivePath, Chdir: outDir, Members: []string{"dir/keep.txt"}}); got.ExitCode != ExitSuccess {
		t.Fatalf("extract exit=%d err=%v", got.ExitCode, got.Err)
	}
	if _, err := os.Stat(filepath.Join(outDir, "dir", "skip.txt")); !os.IsNotExist(err) {
		t.Fatalf("skip.txt should not be extracted, stat err=%v", err)
	}
	b, err := os.ReadFile(filepath.Join(outDir, "dir", "keep.txt"))
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if string(b) != "keep" {
		t.Fatalf("keep.txt = %q, want keep", string(b))
	}
}
