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

## Code Documentation and Commenting

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

**WRONG**

```Go
// Max upload size is 10MB.
const maxUploadSize = 20 << 20

// Cache is a struct that has a map and a mutex.
type Cache struct {
	mu sync.RWMutex
	m  map[string]string
}

// DeleteUser deletes a user.
func DeleteUser(ctx context.Context, userID string) error {
	// ...
	return nil
}

// Loop 3 times
for i := 0; i < 3; i++ {
	// Call charge
	err := charge()
	// If no error break
	if err == nil {
		break
	}
	// If conflict backoff
	if isConflict(err) {
		backoff(i)
	}
}

u, err := r.db.FindUser(id)
if err != nil {
	// Handle error
	return User{}, err
}
```

**RIGHT**

```Go
const (
	// maxUploadSize limits memory usage during multipart parsing.
	maxUploadSize = 10 << 20 // 10 MiB
)

// Cache stores values by key and is safe for concurrent use.
//
// Cache guarantees that for any key, the value returned by Get is either the
// most recent Set for that key or not found.
type Cache struct {
	mu sync.RWMutex
	m  map[string]string
}

// DeleteUser deletes the user and all dependent records.
//
// It returns ErrNotFound if the user does not exist.
// If ctx is canceled, DeleteUser aborts and returns ctx.Err().
func DeleteUser(ctx context.Context, userID string) error {
	// ...
	return nil
}

// We retry on 409 because the provider returns it for idempotency conflicts
// during eventual consistency windows.
for i := 0; i < 3; i++ {
	err := charge()
	if err == nil {
		break
	}
	if !isConflict(err) {
		return err
	}
	backoff(i)
}

u, err := r.db.FindUser(id)
if err != nil {
	// Wrap to preserve the original error while adding query context for logs.
	return User{}, fmt.Errorf("find user %q: %w", id, err)
}
```
