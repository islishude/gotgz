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

### Don't use `aws.String` or similar helper functions to create pointer values from the AWS SDK. Use `new(string)` instead.

```go
new("my-object-key")
```

## Code Documentation and Commenting

- At a minimum every function must be commented with its intended purpose and
  any assumptions that it makes
  - Function comments must always begin with the name of the function per
    [Effective Go](http://golang.org/doc/effective_go.html)
  - Function comments should be complete sentences since they allow a wide
    variety of automated presentations such as [go.dev](https://go.dev)
  - The general rule of thumb is to look at it as if you were completely
    unfamiliar with the code and ask yourself, would this give me enough
    information to understand what this function does and how I'd probably want
    to use it?
- Exported functions should also include detailed information the caller of the
  function will likely need to know and/or understand:

**WRONG**

```Go
// convert a compact uint32 to big.Int
func CompactToBig(compact uint32) *big.Int {
```

**RIGHT**

```Go
// CompactToBig converts a compact representation of a whole number N to a
// big integer.  The representation is similar to IEEE754 floating point
// numbers.
//
// Like IEEE754 floating point, there are three basic components: the sign,
// the exponent, and the mantissa. They are broken out as follows:
//
//        * the most significant 8 bits represent the unsigned base 256 exponent
//        * bit 23 (the 24th bit) represents the sign bit
//        * the least significant 23 bits represent the mantissa
//
//        -------------------------------------------------
//        |   Exponent     |    Sign    |    Mantissa     |
//        -------------------------------------------------
//        | 8 bits [31-24] | 1 bit [23] | 23 bits [22-00] |
//        -------------------------------------------------
//
// The formula to calculate N is:
//         N = (-1^sign) * mantissa * 256^(exponent-3)
//
// This compact form is only used in bitcoin to encode unsigned 256-bit numbers
// which represent difficulty targets, thus there really is not a need for a
// sign bit, but it is implemented here to stay consistent with bitcoind.
func CompactToBig(compact uint32) *big.Int {
```

- Comments in the body of the code are highly encouraged, but they should
  explain the intention of the code as opposed to just calling out the
  obvious

**WRONG**

```Go
// return err if amt is less than 5460
if amt < 5460 {
  return err
}
```

**RIGHT**

```Go
// Treat transactions with amounts less than the amount which is considered dust
// as non-standard.
if amt < 5460 {
  return err
}
```

**NOTE:** The above should really use a constant as opposed to a magic number,
but it was left as a magic number to show how much of a difference a good
comment can make.
