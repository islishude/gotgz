package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"syscall"

	"github.com/islishude/gotgz/packages/cli"
	"github.com/islishude/gotgz/packages/engine"
)

func main() {
	opts, err := cli.Parse(os.Args[1:])
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "gotgz: %v\n", err)
		os.Exit(engine.ExitFatal)
	}

	switch {
	case opts.Help:
		_, _ = fmt.Fprint(os.Stdout, cli.HelpText(filepath.Base(os.Args[0]), buildVersion()))
		os.Exit(0)
	case opts.Version:
		_, _ = fmt.Fprintf(os.Stdout, "%s\n", buildVersion())
		os.Exit(0)
	}

	basectx, cancel := signal.NotifyContext(context.Background(),
		syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	runner, err := engine.New(basectx, os.Stdout, os.Stderr)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "gotgz: %v\n", err)
		os.Exit(engine.ExitFatal)
	}

	result := runner.Run(basectx, opts)
	if result.Err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "gotgz: %v\n", result.Err)
	} else if !result.ProgressEnabled {
		_, _ = fmt.Fprintf(os.Stderr, "gotgz: completed in %s\n", result.Elapsed)
	}
	os.Exit(result.ExitCode)
}

// buildVersion returns the embedded module version for the running binary.
func buildVersion() string {
	info, ok := debug.ReadBuildInfo()
	if !ok || info == nil || info.Main.Version == "" {
		return "devel"
	}
	return info.Main.Version
}
