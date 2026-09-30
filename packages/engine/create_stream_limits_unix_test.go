//go:build unix

package engine

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/islishude/gotgz/packages/cli"
	localstore "github.com/islishude/gotgz/packages/storage/local"
)

func TestStreamingCreateUnderFileLimit(t *testing.T) {
	if os.Getenv("GOTGZ_TEST_LOW_FD") != "1" {
		exe, err := os.Executable()
		if err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command(exe, "-test.run=^TestStreamingCreateUnderFileLimit$")
		cmd.Env = append(os.Environ(), "GOTGZ_TEST_LOW_FD=1")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("low-fd subprocess: %v\n%s", err, out)
		}
		return
	}
	var limit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &limit); err != nil {
		t.Fatal(err)
	}
	limit.Cur = min(limit.Max, 128)
	if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &limit); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	members := make([]string, 180)
	for i := range members {
		members[i] = fmt.Sprintf("member-%03d", i)
		if err := os.WriteFile(filepath.Join(root, members[i]), []byte("payload"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("TMPDIR", root)
	for _, ext := range []string{"tar", "zip"} {
		path := filepath.Join(root, "archive."+ext)
		var output bytes.Buffer
		r := newRunner(&localstore.ArchiveStore{}, nil, nil, &output, io.Discard)
		if result := r.Run(context.Background(), cli.Options{Mode: cli.ModeCreate, Archive: path, Chdir: root, Members: members}); result.ExitCode != ExitSuccess {
			t.Fatalf("create %s: %+v", ext, result)
		}
		if result := r.Run(context.Background(), cli.Options{Mode: cli.ModeList, Archive: path}); result.ExitCode != ExitSuccess {
			t.Fatalf("list %s: %+v", ext, result)
		}
		if got, want := output.String(), strings.Join(members, "\n")+"\n"; got != want {
			t.Fatalf("member order changed: %q", got)
		}
		original, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		last := filepath.Join(root, members[len(members)-1])
		if err := os.Remove(last); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink("/unsafe", last); err != nil {
			t.Fatal(err)
		}
		result := r.Run(context.Background(), cli.Options{Mode: cli.ModeCreate, Archive: path, Chdir: root, Members: members})
		if result.ExitCode != ExitFatal || result.Err == nil {
			t.Fatalf("late validation: %+v", result)
		}
		after, err := os.ReadFile(path)
		if err != nil || !bytes.Equal(after, original) {
			t.Fatalf("failed create replaced existing output: %v", err)
		}
		if spools, err := filepath.Glob(filepath.Join(root, "gotgz-create-stream-*")); err != nil || len(spools) != 0 {
			t.Fatalf("leaked spools: %v, %v", spools, err)
		}
		if outputs, err := filepath.Glob(filepath.Join(root, ".archive.*.gotgz-*")); err != nil || len(outputs) != 0 {
			t.Fatalf("leaked outputs: %v, %v", outputs, err)
		}
		if err := os.Remove(last); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(last, []byte("payload"), 0600); err != nil {
			t.Fatal(err)
		}
	}
}
