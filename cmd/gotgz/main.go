package main

import (
	"context"
	"fmt"
	"os"

	"github.com/islishude/gotgz/internal/cli"
	"github.com/islishude/gotgz/internal/engine"
)

func main() {
	opts, err := cli.Parse(os.Args[1:])
	if err != nil {
		fmt.Fprintf(os.Stderr, "gotgz: %v\n", err)
		os.Exit(engine.ExitFatal)
	}

	runner, err := engine.New(context.Background(), os.Stdout, os.Stderr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "gotgz: %v\n", err)
		os.Exit(engine.ExitFatal)
	}

	result := runner.Run(context.Background(), opts)
	if result.Err != nil {
		fmt.Fprintf(os.Stderr, "gotgz: %v\n", result.Err)
	}
	os.Exit(result.ExitCode)
}
