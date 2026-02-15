package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/islishude/gotgz/internal/cli"
	"github.com/islishude/gotgz/internal/engine"
)

func main() {
	opts, err := cli.Parse(os.Args[1:])
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "gotgz: %v\n", err)
		os.Exit(engine.ExitFatal)
	}
	if opts.Help {
		_, _ = fmt.Fprint(os.Stdout, cli.HelpText(filepath.Base(os.Args[0])))
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

	start := time.Now()
	result := runner.Run(basectx, opts)
	if result.Err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "gotgz: %v\n", result.Err)
	}
	if result.ExitCode == engine.ExitSuccess {
		_, _ = fmt.Fprintf(os.Stderr, "gotgz: completed in %s\n", time.Since(start).Round(time.Millisecond))
	}
	os.Exit(result.ExitCode)
}
