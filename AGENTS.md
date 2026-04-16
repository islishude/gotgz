# Agent Development Guide

## Commands

- **Build:** `make build`
- **Test:** `make test` for the full three-layer suite, `make unit-test` for default unit tests, `make integration-test` for tagged collaboration tests, `make e2e-test` for tagged CLI end-to-end tests
- **Lint:** `make lint`
- **Format:** `make fmt`

Run `make all` to execute all of the above after modifying the code.

## Install missing tools

Linting is done with [golangci-lint](https://golangci-lint.run/).

You can install it with the following command:

```bash
go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest
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

### Don't use `aws.String` or similar helper functions to create pointer values from the AWS SDK. Use `new(expr)` instead.

**Wrong**

```go
// Pointer to a string variable with the value "my-object-key".
aws.String("my-object-key")
// Pointer to a int variable with the value 42.
aws.Int(42)
```

**Right**

```go
new("my-object-key")
new(42)
```

The feature was introduced in Go 1.26.

### Keep files small and focused.

Split code by responsibility whenever possible; do not put too much logic in a single Go file.

### Code Documentation and Commenting

- At a minimum every function must be commented with its intended purpose and
  any assumptions that it makes
  - Function comments must always begin with the name of the function per
    [Effective Go](https://go.dev/doc/effective_go)
  - Function comments should be complete sentences since they allow a wide
    variety of automated presentations such as [go.dev](https://go.dev)
  - The general rule of thumb is to look at it as if you were completely
    unfamiliar with the code and ask yourself, would this give me enough
    information to understand what this function does and how I'd probably want
    to use it?
- Exported functions should also include detailed information the caller of the
  function will likely need to know and/or understand:
- Comments in the body of the code are highly encouraged, but they should
  explain the intention of the code as opposed to just calling out the
  obvious
