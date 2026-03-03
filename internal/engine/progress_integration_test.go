package engine

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/islishude/gotgz/internal/cli"
)

func TestProgressAlwaysForCreateExtractList(t *testing.T) {
	root := t.TempDir()
	srcDir := filepath.Join(root, "src")
	archive := filepath.Join(root, "out.tar")
	outDir := filepath.Join(root, "out")
	if err := os.MkdirAll(srcDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(srcDir, "note.txt"), []byte("progress-check"), 0o644); err != nil {
		t.Fatal(err)
	}

	var createErr bytes.Buffer
	rCreate, err := New(context.Background(), io.Discard, &createErr)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	create := cli.Options{
		Mode:     cli.ModeCreate,
		Archive:  archive,
		Chdir:    root,
		Progress: cli.ProgressAlways,
		Members:  []string{"src"},
	}
	if got := rCreate.Run(context.Background(), create); got.ExitCode != ExitSuccess {
		t.Fatalf("create exit=%d err=%v", got.ExitCode, got.Err)
	}
	if !strings.Contains(createErr.String(), "gotgz:") {
		t.Fatalf("create stderr missing progress output:\n%s", createErr.String())
	}

	var listErr bytes.Buffer
	rList, err := New(context.Background(), io.Discard, &listErr)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	list := cli.Options{Mode: cli.ModeList, Archive: archive, Progress: cli.ProgressAlways}
	if got := rList.Run(context.Background(), list); got.ExitCode != ExitSuccess {
		t.Fatalf("list exit=%d err=%v", got.ExitCode, got.Err)
	}
	for _, item := range []string{
		"gotgz: [....................]   0.0% ",
		"gotgz: [####################] 100.0% ",
	} {
		if !strings.Contains(listErr.String(), item) {
			t.Errorf("list stderr missing progress output:\n%s\nitem:\n%s", listErr.String(), item)
		}
	}

	if err := os.MkdirAll(outDir, 0o755); err != nil {
		t.Fatal(err)
	}
	var extractErr bytes.Buffer
	rExtract, err := New(context.Background(), io.Discard, &extractErr)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	extract := cli.Options{
		Mode:     cli.ModeExtract,
		Archive:  archive,
		Chdir:    outDir,
		Progress: cli.ProgressAlways,
	}
	if got := rExtract.Run(context.Background(), extract); got.ExitCode != ExitSuccess {
		t.Fatalf("extract exit=%d err=%v", got.ExitCode, got.Err)
	}

	for _, item := range []string{
		"gotgz: [....................]   0.0% ",
		"gotgz: [####################] 100.0% ",
	} {
		if !strings.Contains(extractErr.String(), item) {
			t.Errorf("extract stderr missing progress output:\n%s\nitem:\n%s", extractErr.String(), item)
		}
	}
}

func TestProgressNeverDisablesOutput(t *testing.T) {
	root := t.TempDir()
	srcDir := filepath.Join(root, "src")
	archive := filepath.Join(root, "out.tar")
	if err := os.MkdirAll(srcDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(srcDir, "note.txt"), []byte("progress-check"), 0o644); err != nil {
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
		Progress: cli.ProgressNever,
		Members:  []string{"src"},
	}
	if got := r.Run(context.Background(), create); got.ExitCode != ExitSuccess {
		t.Fatalf("create exit=%d err=%v", got.ExitCode, got.Err)
	}
	if got := stderr.String(); got != "" {
		t.Fatalf("expected no progress output, got %q", got)
	}
}

func TestProgressAutoDisablesOnNonTTY(t *testing.T) {
	root := t.TempDir()
	srcDir := filepath.Join(root, "src")
	archive := filepath.Join(root, "out.tar")
	if err := os.MkdirAll(srcDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(srcDir, "note.txt"), []byte("progress-check"), 0o644); err != nil {
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
		Progress: cli.ProgressAuto,
		Members:  []string{"src"},
	}
	if got := r.Run(context.Background(), create); got.ExitCode != ExitSuccess {
		t.Fatalf("create exit=%d err=%v", got.ExitCode, got.Err)
	}
	if got := stderr.String(); got != "" {
		t.Fatalf("expected no progress output on non-tty auto mode, got %q", got)
	}
}

func TestProgressDoesNotPolluteStdoutWhenExtractingToStdout(t *testing.T) {
	root := t.TempDir()
	srcDir := filepath.Join(root, "src")
	archive := filepath.Join(root, "out.tar")
	if err := os.MkdirAll(srcDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(srcDir, "out.txt"), []byte("stdout-payload"), 0o644); err != nil {
		t.Fatal(err)
	}

	rCreate, err := New(context.Background(), io.Discard, io.Discard)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	create := cli.Options{
		Mode:    cli.ModeCreate,
		Archive: archive,
		Chdir:   root,
		Members: []string{"src"},
	}
	if got := rCreate.Run(context.Background(), create); got.ExitCode != ExitSuccess {
		t.Fatalf("create exit=%d err=%v", got.ExitCode, got.Err)
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	rExtract, err := New(context.Background(), &stdout, &stderr)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	extract := cli.Options{
		Mode:     cli.ModeExtract,
		Archive:  archive,
		ToStdout: true,
		Progress: cli.ProgressAlways,
		Members:  []string{"src/out.txt"},
	}
	if got := rExtract.Run(context.Background(), extract); got.ExitCode != ExitSuccess {
		t.Fatalf("extract exit=%d err=%v", got.ExitCode, got.Err)
	}

	if got := stdout.String(); got != "stdout-payload" {
		t.Fatalf("stdout = %q, want %q", got, "stdout-payload")
	}
	for _, item := range []string{
		"gotgz: [....................]   0.0% ",
		"gotgz: [####################] 100.0% ",
	} {
		if !strings.Contains(stderr.String(), item) {
			t.Fatalf("stderr missing progress output:\n%s\nitem:\n%s", stderr.String(), item)
		}
	}
}
