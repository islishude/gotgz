# Agent Development Guide

`gotgz` is a tar-style archive CLI written in Go. It supports local files, S3
objects, and HTTP(S) archive sources, with tar-family compression and ZIP
output. Keep changes small, package-oriented, and consistent with the
user-visible contracts documented in `README.md` and `packages/cli/help.go`.

## Repository map

- `cmd/gotgz`: process entrypoint, help/version behavior, and CLI subprocess tests.
- `packages/cli`: argument parsing, defaults, cross-option validation, and help text.
- `packages/engine`: create, list, extract, progress, archive format, and split-volume orchestration.
- `packages/archive`: tar PAX metadata, xattrs, ACL handling, and platform-specific filesystem helpers.
- `packages/archivepath`: path safety, member/exclude matching, suffixes, and split-volume names.
- `packages/archiveutil`: format detection, metadata, replay, and stream helpers.
- `packages/compress`: tar-family compression readers and writers.
- `packages/locator`: local, stdio, S3 URI/ARN, and HTTP reference parsing.
- `packages/storage/local`, `packages/storage/s3`, `packages/storage/http`: storage backends.
- `Makefile`: local build, formatting, lint, unit, integration, and end-to-end commands.
- `.github/workflows/`: test, release, and multi-architecture container workflows.
- `Dockerfile`, `docker-compose.yaml`: the production image and RustStack S3 test service.

## Working rules

- Check `git status --short` and the relevant diff before editing. Preserve
  existing staged, unstaged, and untracked work; do not reset or revert work
  that is outside the request.
- Trace behavior to current source and tests before changing documentation.
  User-facing flag behavior is implemented in `packages/cli` and rendered by
  `packages/cli/help.go`; do not document a flag that is not accepted there.
- Keep reusable behavior in its existing package. Add or update tests when a
  code change changes a shared package contract.
- Keep generated files, coverage files, temporary plan data, and binaries out
  of commits unless the request explicitly includes them.
- Do not commit or push unless explicitly requested.

## Contracts that must remain intact

- Archive member order is deterministic and follows input order. Planning may
  run concurrently, but it must not change output order or validation timing.
- Local single-file create uses a rollback-safe temporary file and atomic
  publication. A failed create must not replace an existing target. S3 uploads
  must commit only after successful finalization and abort failed multipart
  work.
- Create-time symlinks with absolute or archive-root-escaping targets are
  rejected. Extraction must keep paths and symlink targets inside the chosen
  destination and must not follow an existing symlink in the destination path.
- Tar metadata recovery is best effort after content/path validation: ordinary
  recovery failures produce warnings and exit status 1. Missing or
  type-changed extraction targets fail closed, as do content, archive-format,
  and other path-safety failures, with exit status 2.
- xattrs are Unix-only; ACL preservation is Linux-only. ZIP does not carry the
  tar PAX xattr/ACL contract and must warn when those flags are requested.
- S3 range reads must remain bound to the initial object validator. Do not
  weaken Version ID, ETag, HTTP validator, Content-Range, or exact-length
  checks to make a remote read appear more compatible.
- Split archives use stable `.partNNNN` names and are not a set-level
  transaction. Rebuilding a shorter archive with the same base name does not
  remove stale higher-numbered parts; retain the README warning unless the
  protocol is deliberately changed.

## Local commands

The module declares Go 1.26.1. CI currently tests with Go 1.27 and the Docker
compiler uses Go 1.27.0, so verify the local toolchain before interpreting a
tool failure.

| Command                 | What it runs                                                        | Requirements or notes                                                                        |
| ----------------------- | ------------------------------------------------------------------- | -------------------------------------------------------------------------------------------- |
| `make fmt`              | Formats all Go files with `gofmt -w -s .`                           | Mutates source files.                                                                        |
| `make fmt-check`        | Checks formatting and runs `go fix -diff ./...`                     | Read-only validation.                                                                        |
| `make build`            | Builds `./cmd/gotgz` as `./gotgz`                                   | The binary is ignored by Git.                                                                |
| `make lint`             | Runs `go vet ./...` and `golangci-lint`                             | Requires `golangci-lint`.                                                                    |
| `make unit-test`        | Runs untagged `go test -v -race -count=1 ./...`                     | No Docker required.                                                                          |
| `make integration-test` | Starts RustStack, then runs `go test -race -tags=integration ./...` | Includes untagged tests; requires Docker Compose, uses port 4566, and writes `coverage.txt`. |
| `make e2e-test`         | Runs the tagged CLI subprocess suite in `cmd/gotgz`                 | `go test -race -tags=e2e ./cmd/gotgz`.                                                       |
| `make test`             | Runs `make integration-test` followed by `make e2e-test`            | It does not invoke the standalone `make unit-test` target.                                   |
| `make all`              | Runs `fmt-check`, `build`, `lint`, and `test`                       | Run `make unit-test` separately for a Docker-free unit gate.                                 |
| `make s3mock`           | Starts RustStack without running tests                              | Useful for manual S3 checks.                                                                 |

For normal code completion, use the narrowest relevant check while iterating.
For shared engine, storage, archive, or CLI behavior, the preferred local
boundary is:

```bash
make unit-test
make all
git diff --check
```

If the default Go or lint caches are not writable, use task-scoped writable
caches rather than changing repository configuration:

```bash
GOMODCACHE=/tmp/gotgz-gomod \
GOCACHE=/tmp/gotgz-gocache \
GOTMPDIR=/tmp/gotgz-gotmp \
GOLANGCI_LINT_CACHE=/tmp/gotgz-lint-cache \
make all
```

The integration suite expects `GOTGZ_TEST_S3_ENDPOINT=http://localhost:4566`
and supplies it internally. Direct invocations can use:

```bash
GOTGZ_TEST_S3_ENDPOINT=http://localhost:4566 \
go test -race -count=1 -tags=integration ./...
go test -race -count=1 -tags=e2e ./cmd/gotgz
```

For a platform build check, use the same CGO-free shape as the release config,
for example:

```bash
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build ./cmd/gotgz
CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build ./cmd/gotgz
CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go build ./cmd/gotgz
```

## Documentation and review checklist

When changing behavior:

1. Update the parser/help text, tests, and `README.md` together when a CLI
   contract changes.
2. Explain platform, storage, permission, and known-limitation boundaries;
   distinguish local/mock/RustStack evidence from live AWS or release proof.
3. Check examples against `gotgz --help` and the current implementation.
4. Run `git diff --check`, inspect the final diff, and report any skipped
   Docker, cross-platform, CI, or live-service validation explicitly.

When changing only documentation, validate links and examples without
claiming that code or CI gates were rerun unless they actually were.

## CI and release references

- `.github/workflows/test.yaml` runs on Ubuntu with a RustStack service,
  performs build/format/module checks, and runs the race-enabled `integration`
  and `e2e` suites with coverage.
- `.github/workflows/docker.yml` builds and publishes Linux `amd64` and
  `arm64` images to `ghcr.io/islishude/gotgz`.
- `.github/workflows/release.yml` invokes GoReleaser for main-branch and tag
  pushes. `.goreleaser.yaml` produces CGO-free Linux, Windows, and Darwin
  binaries for the configured architectures, with a Windows ZIP archive.
