package main

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestMainVersionLong(t *testing.T) {
	stdout, stderr, exitCode := runMainProcess(t, "--version")
	if exitCode != 0 {
		t.Fatalf("exit code = %d, want 0; stderr:\n%s", exitCode, stderr)
	}

	want := fmt.Sprintf("%s\n", buildVersion())
	if stdout != want {
		t.Fatalf("stdout = %q, want %q", stdout, want)
	}
	if stderr != "" {
		t.Fatalf("stderr = %q, want empty", stderr)
	}
}

func TestMainVersionShort(t *testing.T) {
	stdout, stderr, exitCode := runMainProcess(t, "-V")
	if exitCode != 0 {
		t.Fatalf("exit code = %d, want 0; stderr:\n%s", exitCode, stderr)
	}

	want := fmt.Sprintf("%s\n", buildVersion())
	if stdout != want {
		t.Fatalf("stdout = %q, want %q", stdout, want)
	}
	if stderr != "" {
		t.Fatalf("stderr = %q, want empty", stderr)
	}
}

func TestMainHelpIncludesVersion(t *testing.T) {
	stdout, stderr, exitCode := runMainProcess(t, "--help")
	if exitCode != 0 {
		t.Fatalf("exit code = %d, want 0; stderr:\n%s", exitCode, stderr)
	}

	header := fmt.Sprintf("gotgz %s - tar-compatible archiver", buildVersion())
	if !strings.HasPrefix(stdout, header) {
		t.Fatalf("stdout header = %q, want prefix %q", stdout, header)
	}
	if !strings.Contains(stdout, "-V, --version") {
		t.Fatalf("stdout = %q, want version flag in help", stdout)
	}
	if stderr != "" {
		t.Fatalf("stderr = %q, want empty", stderr)
	}
}

func TestMainHelpShortIncludesVersion(t *testing.T) {
	stdout, stderr, exitCode := runMainProcess(t, "-h")
	if exitCode != 0 {
		t.Fatalf("exit code = %d, want 0; stderr:\n%s", exitCode, stderr)
	}

	header := fmt.Sprintf("gotgz %s - tar-compatible archiver", buildVersion())
	if !strings.HasPrefix(stdout, header) {
		t.Fatalf("stdout header = %q, want prefix %q", stdout, header)
	}
	if stderr != "" {
		t.Fatalf("stderr = %q, want empty", stderr)
	}
}

// TestMainNoProgressPrintsStandaloneCompletionLine verifies that successful
// runs with --no-progress still report the total elapsed time.
func TestMainNoProgressPrintsStandaloneCompletionLine(t *testing.T) {
	root := t.TempDir()
	srcDir := filepath.Join(root, "src")
	archive := filepath.Join(root, "out.tar")
	if err := os.MkdirAll(srcDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(srcDir, "note.txt"), []byte("payload"), 0o644); err != nil {
		t.Fatal(err)
	}

	_, stderr, exitCode := runMainProcess(t,
		"--create",
		"-f", archive,
		"--directory", root,
		"--no-progress",
		"src",
	)
	if exitCode != 0 {
		t.Fatalf("exit code = %d, want 0; stderr:\n%s", exitCode, stderr)
	}
	if !strings.Contains(stderr, "gotgz: completed in ") {
		t.Fatalf("stderr = %q, want completion line", stderr)
	}
	if !strings.HasSuffix(stderr, "\n") {
		t.Fatalf("stderr = %q, want trailing newline", stderr)
	}
}

// TestMainProgressOmitsStandaloneCompletionLine verifies that progress-enabled
// runs report elapsed time in the progress line instead of a separate
// completion-time message.
func TestMainProgressOmitsStandaloneCompletionLine(t *testing.T) {
	root := t.TempDir()
	srcDir := filepath.Join(root, "src")
	archive := filepath.Join(root, "out.tar")
	if err := os.MkdirAll(srcDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(srcDir, "note.txt"), []byte("payload"), 0o644); err != nil {
		t.Fatal(err)
	}

	_, stderr, exitCode := runMainProcess(t,
		"--create",
		"-f", archive,
		"--directory", root,
		"--progress",
		"src",
	)
	if exitCode != 0 {
		t.Fatalf("exit code = %d, want 0; stderr:\n%s", exitCode, stderr)
	}
	if !strings.Contains(stderr, "gotgz:") {
		t.Fatalf("stderr = %q, want progress output", stderr)
	}
	if !strings.Contains(stderr, "ETA ") {
		t.Fatalf("stderr = %q, want ETA in progress output", stderr)
	}
	if !strings.Contains(stderr, "elapsed ") {
		t.Fatalf("stderr = %q, want elapsed in progress output", stderr)
	}
	if strings.Contains(stderr, "completed in") {
		t.Fatalf("stderr = %q, did not expect completion line", stderr)
	}
}

// TestMainProcess runs the real CLI entrypoint inside the test binary so the
// parent test can assert on exit status and output.
func TestMainProcess(t *testing.T) {
	if os.Getenv("GOTGZ_TEST_MAIN_PROCESS") != "1" {
		return
	}

	separator := -1
	for i, arg := range os.Args {
		if arg == "--" {
			separator = i
			break
		}
	}
	if separator < 0 {
		t.Fatal("missing argument separator")
	}

	os.Args = append([]string{"gotgz"}, os.Args[separator+1:]...)
	main()
}

// runMainProcess executes the CLI entrypoint in a subprocess and returns its
// stdout, stderr, and process exit code.
func runMainProcess(t *testing.T, args ...string) (string, string, int) {
	t.Helper()

	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable() error = %v", err)
	}

	cmd := exec.Command(exe, append([]string{"-test.run=TestMainProcess", "--"}, args...)...)
	cmd.Env = append(os.Environ(), "GOTGZ_TEST_MAIN_PROCESS=1")

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err = cmd.Run()
	if err == nil {
		return stdout.String(), stderr.String(), 0
	}

	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("cmd.Run() error = %v", err)
	}

	return stdout.String(), stderr.String(), exitErr.ExitCode()
}
