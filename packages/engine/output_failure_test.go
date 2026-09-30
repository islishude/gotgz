package engine

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/islishude/gotgz/packages/cli"
	localstore "github.com/islishude/gotgz/packages/storage/local"
)

type failedOutput struct{ err error }

func (w failedOutput) Write([]byte) (int, error) { return 0, w.err }

func TestListPropagatesOutputFailure(t *testing.T) {
	for _, format := range []string{"tar", "zip"} {
		t.Run(format, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "archive."+format)
			payload := tarArchiveBytes(t, map[string]string{"member": "payload"})
			if format == "zip" {
				payload = zipArchiveBytes(t, map[string]string{"member": "payload"})
			}
			if err := os.WriteFile(path, payload, 0600); err != nil {
				t.Fatal(err)
			}
			want := errors.New("output unavailable")
			runner := newRunner(&localstore.ArchiveStore{}, nil, nil, failedOutput{want}, io.Discard)
			result := runner.Run(context.Background(), cli.Options{Mode: cli.ModeList, Archive: path})
			if result.ExitCode != ExitFatal || !errors.Is(result.Err, want) {
				t.Fatalf("Run() = %+v, want fatal output error", result)
			}
		})
	}
}

func TestCreateVerboseOutputFailurePreservesDestination(t *testing.T) {
	for _, format := range []string{"tar", "zip"} {
		t.Run(format, func(t *testing.T) {
			root := t.TempDir()
			archive := filepath.Join(root, "archive."+format)
			original := []byte("existing archive")
			if err := os.WriteFile(archive, original, 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(root, "input"), []byte("payload"), 0600); err != nil {
				t.Fatal(err)
			}
			want := errors.New("verbose output unavailable")
			runner := newRunner(&localstore.ArchiveStore{}, nil, nil, failedOutput{want}, io.Discard)
			result := runner.Run(context.Background(), cli.Options{Mode: cli.ModeCreate, Archive: archive, Chdir: root, Members: []string{"input"}, Verbose: true})
			if result.ExitCode != ExitFatal || !errors.Is(result.Err, want) {
				t.Fatalf("Run() = %+v", result)
			}
			after, err := os.ReadFile(archive)
			if err != nil || !bytes.Equal(after, original) {
				t.Fatalf("destination changed: %q, %v", after, err)
			}
		})
	}
}
