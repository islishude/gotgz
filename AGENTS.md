# Agent Development Guide

## Commands

- **Build:** `make build`
- **Test:** `make test` for the full three-layer suite, `make unit-test` for default unit tests, `make integration-test` for tagged collaboration tests(requires docker and docker-compose), `make e2e-test` for tagged CLI end-to-end tests
- **Lint:** `make lint` (requires golangci-lint)
- **Format:** `make fmt`

Run `make all` to execute all of the above after modifying the code.

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

- Use comments to explain the "why" behind complex logic, not the "what". The code itself should be clear enough to convey the "what".
- Use Go's documentation comment style (starting with the name of the function, type, or variable being documented) for public APIs. For example:

```go
// GetObject retrieves an object from the specified S3 bucket.
func GetObject(bucketName, objectKey string) (*s3.GetObjectOutput, error) {
    // implementation
}
```

- For internal functions and complex logic, use inline comments to clarify the intent and reasoning. For example:

```go
// Check if the object is a directory by looking for a trailing slash in the key.
if strings.HasSuffix(objectKey, "/") {
    // This is a directory, handle accordingly
}
```

- Avoid redundant comments that simply restate what the code does. Instead, focus on providing insights into the design decisions and potential pitfalls.
- Ensure that comments are kept up-to-date with code changes to prevent confusion. Outdated comments can be more harmful than no comments at all.
- Use comments to indicate any assumptions, limitations, or edge cases that the code handles. This can help other developers understand the context and constraints of the implementation.
