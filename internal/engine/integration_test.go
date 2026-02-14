package engine

import (
	"context"
	"io"
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
