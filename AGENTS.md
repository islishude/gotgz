# Agent Development Guide

A file for [guiding coding agents](https://agents.md/).

## Commands

- **Build:** `make build`
- **Test:** `make test`
- **Lint:** `make lint`
- **Format:** `make fmt`

## Install missing tools

Linting is done with [golangci-lint](https://golangci-lint.run/).

You can install it with the following command:

```bash
go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest
```

Formatting is done with `goimports`, which is included in the Go toolchain.

You can install it with the following command:

```bash
go install golang.org/x/tools/cmd/goimports@latest
```

Testing requires Docker Compose. You can install it with the following command:

```
# For Linux
sudo apt-get install docker-compose-plugin
# For macOS with Homebrew
brew install docker docker-compose
```

## Code Style

### Check the error return values of all functions.

If you want to ignore an error, assign it to the blank identifier (`_`) or add a comment `// nolint: errcheck` at the end of the line.

```go
_, _ = fmt.Fprintf(r.stderr, "gotgz: warning: %s: %v\n", hdr.Name, err) // nolint: errcheck
```

```go
defer func() {
    if err := r.Close(); err != nil {
        // handle error, e.g. log it
    }
}()
```

### Don't use `aws.String` or similar helper functions to create pointer values from the AWS SDK. Use `new(string)` instead.

```go
new("my-object-key")
```
