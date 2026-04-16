package engine

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/islishude/gotgz/packages/cli"
	"github.com/islishude/gotgz/packages/locator"
)

func TestCreateExtractLocalSplitTarUsesParallelWorkers(t *testing.T) {
	root := t.TempDir()
	archive := filepath.Join(root, "bundle.tar")
	out := filepath.Join(root, "out")

	if err := os.WriteFile(filepath.Join(root, "one.txt"), []byte("one"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "two.txt"), []byte("two"), 0o644); err != nil {
		t.Fatal(err)
	}

	rCreate := newLocalSplitExtractTestRunner()
	create := cli.Options{
		Mode:           cli.ModeCreate,
		Archive:        archive,
		Chdir:          root,
		SplitSizeBytes: 1,
		Members:        []string{"one.txt", "two.txt"},
	}
	if got := rCreate.Run(context.Background(), create); got.ExitCode != ExitSuccess {
		t.Fatalf("create exit=%d err=%v", got.ExitCode, got.Err)
	}

	firstPart := filepath.Join(root, "bundle.part0001.tar")
	rExtract := newLocalSplitExtractTestRunner()
	var plan splitExtractPlan
	started := make(chan int, 2)
	rExtract.splitExtractHooks = &splitExtractHooks{
		onPlan: func(got splitExtractPlan) {
			plan = got
		},
		onParallelWorkerStart: func(index int, _ locator.Ref) {
			started <- index
		},
	}

	if err := os.MkdirAll(out, 0o755); err != nil {
		t.Fatal(err)
	}
	extract := cli.Options{Mode: cli.ModeExtract, Archive: firstPart, Chdir: out}
	if got := rExtract.Run(context.Background(), extract); got.ExitCode != ExitSuccess {
		t.Fatalf("extract exit=%d err=%v", got.ExitCode, got.Err)
	}

	if !plan.parallel {
		t.Fatalf("parallel = false, want true")
	}
	if len(started) != 2 {
		t.Fatalf("parallel worker starts = %d, want 2", len(started))
	}

	for _, tc := range []struct {
		name string
		want string
	}{
		{name: "one.txt", want: "one"},
		{name: "two.txt", want: "two"},
	} {
		b, err := os.ReadFile(filepath.Join(out, tc.name))
		if err != nil {
			t.Fatalf("read %s: %v", tc.name, err)
		}
		if string(b) != tc.want {
			t.Fatalf("%s = %q, want %q", tc.name, string(b), tc.want)
		}
	}
}

func TestCreateExtractLocalSplitZipUsesParallelWorkers(t *testing.T) {
	root := t.TempDir()
	archive := filepath.Join(root, "bundle.zip")
	out := filepath.Join(root, "out")

	if err := os.WriteFile(filepath.Join(root, "one.txt"), []byte("one"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "two.txt"), []byte("two"), 0o644); err != nil {
		t.Fatal(err)
	}

	rCreate := newLocalSplitExtractTestRunner()
	create := cli.Options{
		Mode:           cli.ModeCreate,
		Archive:        archive,
		Chdir:          root,
		SplitSizeBytes: 1,
		Members:        []string{"one.txt", "two.txt"},
	}
	if got := rCreate.Run(context.Background(), create); got.ExitCode != ExitSuccess {
		t.Fatalf("create exit=%d err=%v", got.ExitCode, got.Err)
	}

	firstPart := filepath.Join(root, "bundle.part0001.zip")
	rExtract := newLocalSplitExtractTestRunner()
	var plan splitExtractPlan
	started := make(chan int, 2)
	rExtract.splitExtractHooks = &splitExtractHooks{
		onPlan: func(got splitExtractPlan) {
			plan = got
		},
		onParallelWorkerStart: func(index int, _ locator.Ref) {
			started <- index
		},
	}

	if err := os.MkdirAll(out, 0o755); err != nil {
		t.Fatal(err)
	}
	extract := cli.Options{Mode: cli.ModeExtract, Archive: firstPart, Chdir: out}
	if got := rExtract.Run(context.Background(), extract); got.ExitCode != ExitSuccess {
		t.Fatalf("extract exit=%d err=%v", got.ExitCode, got.Err)
	}

	if !plan.parallel {
		t.Fatalf("parallel = false, want true")
	}
	if len(started) != 2 {
		t.Fatalf("parallel worker starts = %d, want 2", len(started))
	}

	for _, tc := range []struct {
		name string
		want string
	}{
		{name: "one.txt", want: "one"},
		{name: "two.txt", want: "two"},
	} {
		b, err := os.ReadFile(filepath.Join(out, tc.name))
		if err != nil {
			t.Fatalf("read %s: %v", tc.name, err)
		}
		if string(b) != tc.want {
			t.Fatalf("%s = %q, want %q", tc.name, string(b), tc.want)
		}
	}
}

func TestExtractLocalSplitTarCrossVolumeHardlinkFallsBackToSerial(t *testing.T) {
	root := t.TempDir()
	firstPart := filepath.Join(root, "bundle.part0001.tar")
	secondPart := filepath.Join(root, "bundle.part0002.tar")
	out := filepath.Join(root, "out")

	if err := os.WriteFile(firstPart, tarArchiveBytes(t, map[string]string{"dir/original.txt": "payload"}), 0o644); err != nil {
		t.Fatalf("write %s: %v", firstPart, err)
	}
	if err := os.WriteFile(secondPart, tarArchiveHardLinkBytes(t, "dir/alias.txt", "dir/original.txt"), 0o644); err != nil {
		t.Fatalf("write %s: %v", secondPart, err)
	}

	rExtract := newLocalSplitExtractTestRunner()
	var plan splitExtractPlan
	started := make(chan int, 2)
	rExtract.splitExtractHooks = &splitExtractHooks{
		onPlan: func(got splitExtractPlan) {
			plan = got
		},
		onParallelWorkerStart: func(index int, _ locator.Ref) {
			started <- index
		},
	}

	if err := os.MkdirAll(out, 0o755); err != nil {
		t.Fatal(err)
	}
	extract := cli.Options{Mode: cli.ModeExtract, Archive: firstPart, Chdir: out}
	if got := rExtract.Run(context.Background(), extract); got.ExitCode != ExitSuccess {
		t.Fatalf("extract exit=%d err=%v", got.ExitCode, got.Err)
	}

	if plan.parallel {
		t.Fatalf("parallel = true, want false")
	}
	if plan.serialReason != splitExtractSerialReasonLocalHardlink {
		t.Fatalf("serialReason = %q, want %q", plan.serialReason, splitExtractSerialReasonLocalHardlink)
	}
	if len(started) != 0 {
		t.Fatalf("parallel worker starts = %d, want 0", len(started))
	}

	original := filepath.Join(out, "dir", "original.txt")
	alias := filepath.Join(out, "dir", "alias.txt")
	origInfo, err := os.Stat(original)
	if err != nil {
		t.Fatalf("stat original: %v", err)
	}
	aliasInfo, err := os.Stat(alias)
	if err != nil {
		t.Fatalf("stat alias: %v", err)
	}
	if !os.SameFile(origInfo, aliasInfo) {
		t.Fatalf("expected %s to be a hard link to %s", alias, original)
	}
}
