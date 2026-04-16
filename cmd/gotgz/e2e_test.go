//go:build e2e

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestE2ETarCreateListExtract(t *testing.T) {
	root := t.TempDir()
	srcDir := filepath.Join(root, "src")
	archive := filepath.Join(root, "bundle.tar")
	outDir := filepath.Join(root, "out")
	if err := os.MkdirAll(filepath.Join(srcDir, "nested"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(srcDir, "nested", "note.txt"), []byte("payload"), 0o644); err != nil {
		t.Fatal(err)
	}

	stdout, stderr, exitCode := runMainProcess(t, "--create", "-f", archive, "--directory", root, "src")
	if exitCode != 0 || stdout != "" {
		t.Fatalf("create stdout=%q stderr=%q exit=%d", stdout, stderr, exitCode)
	}
	if !strings.Contains(stderr, "gotgz: completed in ") {
		t.Fatalf("create stderr=%q", stderr)
	}

	stdout, stderr, exitCode = runMainProcess(t, "--list", "-f", archive)
	if exitCode != 0 {
		t.Fatalf("list exit=%d stderr=%q", exitCode, stderr)
	}
	if !strings.Contains(stdout, "src/nested/note.txt") {
		t.Fatalf("list stdout=%q", stdout)
	}

	stdout, stderr, exitCode = runMainProcess(t, "--extract", "-f", archive, "--directory", outDir)
	if exitCode != 0 || stdout != "" {
		t.Fatalf("extract stdout=%q stderr=%q exit=%d", stdout, stderr, exitCode)
	}
	if !strings.Contains(stderr, "gotgz: completed in ") {
		t.Fatalf("extract stderr=%q", stderr)
	}
	if got, err := os.ReadFile(filepath.Join(outDir, "src", "nested", "note.txt")); err != nil || string(got) != "payload" {
		t.Fatalf("extracted file err=%v body=%q", err, string(got))
	}
}

func TestE2EZipCreateExtract(t *testing.T) {
	root := t.TempDir()
	srcDir := filepath.Join(root, "src")
	archive := filepath.Join(root, "bundle.zip")
	outDir := filepath.Join(root, "out")
	if err := os.MkdirAll(srcDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(srcDir, "hello.txt"), []byte("zip-payload"), 0o644); err != nil {
		t.Fatal(err)
	}

	_, stderr, exitCode := runMainProcess(t, "--create", "-f", archive, "--directory", root, "src")
	if exitCode != 0 {
		t.Fatalf("create exit=%d stderr=%q", exitCode, stderr)
	}
	_, stderr, exitCode = runMainProcess(t, "--extract", "-f", archive, "--directory", outDir)
	if exitCode != 0 {
		t.Fatalf("extract exit=%d stderr=%q", exitCode, stderr)
	}
	if got, err := os.ReadFile(filepath.Join(outDir, "src", "hello.txt")); err != nil || string(got) != "zip-payload" {
		t.Fatalf("extracted file err=%v body=%q", err, string(got))
	}
}

func TestE2EFailureExitCode(t *testing.T) {
	_, stderr, exitCode := runMainProcess(t, "--extract", "-f", filepath.Join(t.TempDir(), "missing.tar"), "--directory", t.TempDir())
	if exitCode != 2 {
		t.Fatalf("exitCode = %d, want 2; stderr=%q", exitCode, stderr)
	}
	if !strings.Contains(stderr, "missing.tar") {
		t.Fatalf("stderr = %q, want archive path", stderr)
	}
}

func TestE2EProgressContracts(t *testing.T) {
	tests := []struct {
		name     string
		flag     string
		want     []string
		dontWant []string
	}{
		{name: "no progress", flag: "--no-progress", want: []string{"gotgz: completed in "}},
		{name: "progress", flag: "--progress", want: []string{"gotgz:", "ETA ", "elapsed "}, dontWant: []string{"completed in"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()
			srcDir := filepath.Join(root, "src")
			archive := filepath.Join(root, fmt.Sprintf("%s.tar", strings.ReplaceAll(tt.name, " ", "-")))
			if err := os.MkdirAll(srcDir, 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(srcDir, "note.txt"), []byte("payload"), 0o644); err != nil {
				t.Fatal(err)
			}

			_, stderr, exitCode := runMainProcess(t, "--create", "-f", archive, "--directory", root, tt.flag, "src")
			if exitCode != 0 {
				t.Fatalf("exit=%d stderr=%q", exitCode, stderr)
			}
			for _, want := range tt.want {
				if !strings.Contains(stderr, want) {
					t.Fatalf("stderr = %q, want %q", stderr, want)
				}
			}
			for _, unwanted := range tt.dontWant {
				if strings.Contains(stderr, unwanted) {
					t.Fatalf("stderr = %q, did not want %q", stderr, unwanted)
				}
			}
		})
	}
}
