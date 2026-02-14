# Agent Development Guide

A file for [guiding coding agents](https://agents.md/).

## Commands

- **Build:** `make build`
- **Test:** `make test`

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

### Don't use `aws.String` or similar helper functions from the AWS SDK. use `new(string)` instead.

```go
new("my-object-key")
```
