# Agent Development Guide

This repository contains the `gotgz` Go CLI. Keep changes small, focused, and aligned with the existing package boundaries.

## Repository Layout

- `cmd/gotgz`: CLI entrypoint and command-level tests.
- `packages/*`: reusable implementation packages.
- `internal/compress`: local replacement for `github.com/dsnet/compress`.
- `docker-compose.yaml`: services used by integration tests.

## Commands

- Build: `make build`
- Format: `make fmt`
- Lint: `make lint` (requires `golangci-lint`)
- Unit tests: `make unit-test`
- Integration tests: `make integration-test` (requires Docker and Docker Compose)
- CLI end-to-end tests: `make e2e-test`
- Full test suite: `make test`
- Full validation after code changes: `make all`

Prefer the narrowest useful command while iterating. Before finishing a code change, run the relevant tests; use `make all` when the change affects shared behavior or release confidence.

## Go Style

### Handle Errors Explicitly

Check error return values from all functions. If an error is intentionally ignored, make that choice explicit with the blank identifier or an inline `errcheck` suppression.

```go
_, _ = fmt.Fprintf(r.stderr, "gotgz: warning: %s: %v\n", hdr.Name, err) // nolint: errcheck
```

For cleanup, prefer a deferred closure when the close error matters.

```go
defer func() {
	if err := r.Close(); err != nil {
		// Handle or report the close failure.
	}
}()
```

### Avoid AWS Pointer Helpers

Do not use AWS SDK helper functions such as `aws.String`, `aws.Int`, or similar helpers to allocate pointer values. This project targets Go 1.26, so use `new(expr)` instead.

Wrong:

```go
aws.String("my-object-key")
aws.Int(42)
```

Right:

```go
new("my-object-key")
new(42)
```

### Keep Files Focused

Keep files small and organized around one responsibility. Split code when a file starts mixing unrelated concerns or becomes difficult to scan.

### Comment With Intent

- Public Go APIs should use documentation comments that start with the exported identifier name.
- Internal comments should explain intent, constraints, assumptions, or edge cases.
- Avoid comments that merely repeat the code.
- Update comments when changing the behavior they describe.

Example:

```go
// GetObject retrieves an object from the specified S3 bucket.
func GetObject(bucketName, objectKey string) (*s3.GetObjectOutput, error) {
	// ...
}
```

## Change Discipline

- Preserve existing behavior unless the task requires changing it.
- Do not revert unrelated local changes.
- Keep generated artifacts and binaries out of commits unless the task explicitly requires them.
- Add or update tests when behavior changes, especially in shared packages under `packages/*`.
