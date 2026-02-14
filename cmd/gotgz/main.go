package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/islishude/gotgz/internal/cli"
	"github.com/islishude/gotgz/internal/engine"
)

func main() {
	opts, err := cli.Parse(os.Args[1:])
	if err != nil {
		fmt.Fprintf(os.Stderr, "gotgz: %v\n", err)
		os.Exit(engine.ExitFatal)
	}

	basectx, cancel := signal.NotifyContext(context.Background(),
		syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	runner, err := engine.New(basectx, os.Stdout, os.Stderr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "gotgz: %v\n", err)
		os.Exit(engine.ExitFatal)
	}

	result := runner.Run(basectx, opts)
	if result.Err != nil {
		fmt.Fprintf(os.Stderr, "gotgz: %v\n", result.Err)
	}
	os.Exit(result.ExitCode)
}
