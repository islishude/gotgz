package main

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
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
