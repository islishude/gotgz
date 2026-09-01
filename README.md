# gotgz

`gotgz` is a tar-style archive CLI written in Go. It creates and reads tar
archives, creates and reads ZIP archives, supports gzip/bzip2/xz/zstd/lz4
compression, and treats AWS S3 as a first-class source or destination. HTTP
and HTTPS are supported as archive sources for listing and extraction.

It follows common GNU `tar` conventions, but it is not a complete replacement
for every GNU `tar` option. Run `gotgz --help` for the command-line contract
provided by the installed binary.

## Features

- Create, extract, and list archives with `-c`, `-x`, and `-t`.
- Tar-style bundled flags such as `-cvf` and the legacy `cvf` form.
- Local paths, `-` for stdin/stdout, S3 URIs and object ARNs, and HTTP(S)
  archive sources.
- ZIP plus tar-family output with automatic format/compression detection.
- PAX metadata, optional extended attributes, Linux ACL xattrs, ownership and
  permission controls, safe symlinks, hardlinks, and path stripping.
- Glob and exclude filtering with deterministic archive member order.
- Independently readable split volumes named `.partNNNN`.
- Progress reporting, S3 server-side encryption, Cache-Control, and object
  tags.

## Installation

Build from source with Go 1.26.1 or newer:

```bash
go install github.com/islishude/gotgz/cmd/gotgz@latest
```

Pre-built binaries are available on the [releases page](https://github.com/islishude/gotgz/releases/latest).

The published container image is available at `ghcr.io/islishude/gotgz`:

```bash
docker run --rm -v "$PWD:/data" ghcr.io/islishude/gotgz --help
```

## Command shape

```text
gotgz -c -f <archive> [options] <member>...
gotgz -x -f <archive> [options] [member]...
gotgz -t -f <archive> [options] [member]...
gotgz [bundled flags] <archive> [options] [member]...
```

`-f` is required. In create mode, member arguments are local paths or S3
objects. In list/extract mode, member arguments select archive entries. `-C`
is the source directory in create mode and the extraction target (a local path
or S3 prefix) in extract mode; `--cd` and `--directory` are aliases.

Options must appear before the first member argument. Use `--` when a member
name begins with `-`.

## Quick start

### Create

```bash
# Local files to an uncompressed tar archive
gotgz -cvf backup.tar project/ README.md

# Format and compression inferred from the final filename
gotgz -cvf backup.tar.gz project/
gotgz -cvf backup.tar.zst project/
gotgz -cvf backup.zip project/

# Explicit compression flags
gotgz -cvzf backup.tar.gz project/
gotgz --zstd -cvf backup.tar.zst project/

# Change the source directory
gotgz -cvf backup.tar -C /srv/app .

# Add a date or custom suffix before format inference
gotgz -cvf backup.tar.gz --suffix date project/
gotgz -cvf backup.tar.gz --suffix daily project/

# Write an archive to S3
gotgz -cvf s3://my-bucket/backups/backup.tar.gz project/

# Include S3 metadata, Cache-Control, and tags
gotgz -cvf 's3://my-bucket/backups/backup.tar.gz?team=platform' \
  --s3-cache-control 'max-age=3600,public' \
  --s3-tag team=platform --s3-tag environment=prod project/

# Use S3 objects as archive members
gotgz -cvf backup.tar s3://my-bucket/data/one.txt s3://my-bucket/data/two.txt
```

Creating an archive with no member arguments is rejected. A local single-file
destination is finalized through a temporary file and atomic rename; an error
does not replace an existing target. S3 single-object writes are committed
only after archive finalization.

### Extract

```bash
# Local archive to a directory
gotgz -xvf backup.tar.gz -C /tmp/restore

# ZIP archive to a directory
gotgz -xvf backup.zip -C /tmp/restore

# Select one member or subtree
gotgz -xvf backup.tar.gz -C /tmp/restore project/

# Extract regular-file data to stdout
gotgz -xOf backup.tar.gz project/config.yaml

# Extract from S3 or HTTP(S)
gotgz -xvf s3://my-bucket/backups/backup.tar.gz -C /tmp/restore
gotgz -xvf https://example.com/backups/backup.tar.gz -C /tmp/restore

# Extract entries to an S3 prefix
gotgz -xvf backup.tar.gz -C s3://my-bucket/restored/
```

### List

```bash
gotgz -tf backup.tar.gz
gotgz -tf backup.zip
gotgz -tf s3://my-bucket/backups/backup.tar.gz
gotgz -tf https://example.com/backups/backup.tar.gz
```

Use `-f -` to read an archive from stdin or, in create mode, write it to
stdout. `--suffix` and `--split-size` are not valid with `-f -`.

## Formats and compression

Create mode selects ZIP when the final archive name ends in `.zip`. Other
names select tar; recognized suffixes select compression as follows:

| Suffixes                             | Compression      |
| ------------------------------------ | ---------------- |
| `.tar.gz`, `.tgz`, `.gz`             | gzip             |
| `.tar.bz2`, `.tbz2`, `.tbz`, `.bz2`  | bzip2            |
| `.tar.xz`, `.txz`, `.xz`             | xz               |
| `.tar.zst`, `.tzst`, `.zst`, `.zstd` | zstd             |
| `.tar.lz4`, `.tlz4`, `.lz4`          | lz4              |
| `.tar`, `.tape`                      | uncompressed tar |

Unknown create-time suffixes fall back to uncompressed tar. If an explicit
tar compression flag is supplied, it must agree with the filename suffix;
the exception is `-f -`, which has no filename hint. `--suffix` is applied
before this decision and must be a filename-only value. The built-in `date`
suffix uses `YYYYMMDD` (`20060102`).

Compression flags are:

| Flag                       | Compression |
| -------------------------- | ----------- |
| `-z`, `--gzip`, `--gunzip` | gzip        |
| `-j`, `--bzip`, `--bzip2`  | bzip2       |
| `-J`, `--xz`               | xz          |
| `--zstd`                   | zstd        |
| `--lz4`                    | lz4         |

`--compression-level 1..9` controls tar compression and maps to ZIP Deflate
when creating a ZIP archive. When reading, gotgz detects compression from
magic bytes first, then the filename, then the content type.

ZIP does not use tar compression or PAX metadata. On ZIP list/extract,
tar-only compression flags, `--xattrs`, `--acl`, `--same-owner`, and
`--numeric-owner` are ignored with warnings. `--same-permissions` remains a
permission policy for local extraction.

## Filtering and path handling

`--exclude <pattern>` may be repeated. `--exclude-from <file>` reads one
pattern per line; blank lines and lines beginning with `#` are ignored. The
same matching rules apply to create, list, and extract.

Without `--wildcards`, a member argument is an exact path selector and a
selected directory includes its descendants. With `--wildcards`:

- `*`, `?`, and bracket expressions match within one path segment.
- `**` is the recursive path segment and can cross directories.
- A pattern without `/`, such as `*.log`, matches basenames at any depth.
- A directory matched by a glob does not automatically include its
  descendants; use a pattern such as `dir/**` when that is intended.

Examples:

```bash
gotgz -tf backup.tar --wildcards 'src/*.go'
gotgz -xvf backup.tar --wildcards 'src/**' -C /tmp/restore
gotgz -cvf backup.tar --exclude='*.log' --exclude-from=exclude.txt project/
```

`--strip-components <count>` removes leading path elements during extraction.
Entries that become empty are skipped. Tar hardlink targets are transformed by
the same count; a target removed by stripping is skipped with a warning.

## Metadata and safety

Tar output uses PAX headers. `--xattrs` preserves extended attributes and
`--acl` preserves the Linux POSIX/NFSv4 ACL xattrs recognized by gotgz. These
options are disabled by default. xattrs are available on Unix builds; ACL
preservation is available on Linux builds. Unsupported platforms warn and
continue, returning exit status 1.

For local extraction:

- `--same-owner` and `--no-same-owner` control archive UID/GID restoration.
- `--same-permissions` and `--no-same-permissions` control mode restoration
  and umask handling.
- `--numeric-owner` is accepted for tar compatibility but currently does not
  change behavior.
- File content and path-safety failures are fatal. Ordinary metadata,
  ownership, permission, timestamp, and xattr/ACL restoration failures are
  warnings after the content has been written; a missing or type-changed
  extraction target fails closed during directory revalidation.

Creation rejects empty, absolute, or archive-root-escaping symlink targets.
Extraction keeps archive paths below the requested destination, rejects
symlink traversal, and validates relative symlink targets before creation.
Regular-file reads are bounded to the size observed for the entry; growth is
not included and truncation fails rather than silently producing a shorter
archive.

## Split archives

Use `--split-size` in create mode to write complete `.partNNNN` volumes:

```bash
gotgz -cvf backup.tar.gz --split-size 2GiB project/
gotgz -cvf backup.zip --split-size 512MiB project/
gotgz -cvf s3://my-bucket/backups/backup.tar.gz \
  --split-size 512MiB project/
```

Supported split output is ZIP plus uncompressed tar, gzip, bzip2, zstd, and
lz4 tar output. xz, stdout, and HTTP targets are not supported for split
creation. Size values accept bytes and binary or decimal units such as `2GiB`,
`512MiB`, `1GB`, and `100M`.

Open the first volume for list or extract; gotgz discovers the remaining local
files or S3 objects and processes them in numeric order:

```bash
gotgz -tf backup.part0001.tar.gz
gotgz -xvf backup.part0001.zip -C /tmp/restore
```

Important limitation: a shorter create with the same base name does not delete
old higher-numbered volumes. Since list/extract discovers every continuous
volume beginning at `part0001`, stale tail volumes can be read as part of the
archive. Use a new base name, or have an operator remove the complete old
split group before creating a replacement. Manual S3 cleanup requires
`s3:DeleteObject`; gotgz does not delete old parts. A split create is not a
set-level transaction, so verify the expected part count after an interrupted
run.

## S3

### Addressing

S3 URIs and supported object/access-point ARNs can be used as archive sources,
destinations, or create-mode member arguments:

```bash
gotgz -tf s3://my-bucket/path/to/archive.tar
gotgz -tf arn:aws:s3:::my-bucket/path/to/archive.tar
gotgz -tf arn:aws:s3:us-west-2:123456789012:accesspoint/myap/object/path/to/archive.tar
gotgz -cvf archive.tar s3://my-bucket/path/to/file.txt
```

Query parameters on an S3 URI become user metadata on uploaded archive
objects. Use `--s3-cache-control` for the Cache-Control header and repeat
`--s3-tag key=value` for object tags. These options apply to S3 archive writes
and extraction-to-S3 targets.

### Permissions

The usual data-plane permissions are:

| Workflow                                     | Permissions                                           |
| -------------------------------------------- | ----------------------------------------------------- |
| Read an S3 archive or create from S3 members | `s3:GetObject`                                        |
| Write an archive or extract entries to S3    | `s3:PutObject`, `s3:AbortMultipartUpload`             |
| Use `--s3-tag`                               | `s3:PutObjectTagging` in addition to write permission |
| Read a split archive from S3                 | `s3:ListBucket` for sibling discovery                 |

`HeadObject` is covered by `s3:GetObject`; there is no separate
`s3:HeadObject` IAM action. SSE-KMS also requires the appropriate permissions
on the customer-managed KMS key, commonly `kms:Decrypt` and
`kms:GenerateDataKey`.

S3 reads begin with object metadata and use bounded concurrent ranged reads for
large objects. Non-empty objects must expose a Version ID or ETag; each range
is fenced to that object version or validator. Uploads below 16 MiB use
`PutObject`; uploads of 16 MiB or more use bounded multipart uploads and abort
failed multipart work.

### S3 environment

The AWS SDK default configuration is used, including standard variables such
as `AWS_REGION`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and
`AWS_ENDPOINT_URL`. gotgz-specific settings are:

| Variable                  | Meaning                                               | Default  |
| ------------------------- | ----------------------------------------------------- | -------- |
| `GOTGZ_S3_SSE`            | `AES256`/`sse-s3`, `aws:kms`/`sse-kms`, or `none`     | `AES256` |
| `GOTGZ_S3_SSE_KMS_KEY_ID` | KMS key ID used with KMS encryption                   | unset    |
| `GOTGZ_S3_PART_SIZE_MB`   | Transfer part size in MiB                             | `16`     |
| `GOTGZ_S3_CONCURRENCY`    | Concurrent upload/download parts                      | `4`      |
| `GOTGZ_S3_MAX_RETRIES`    | Total attempts for SDK requests and ranged body reads | `3`      |
| `GOTGZ_S3_USE_PATH_STYLE` | Enable path-style addressing for emulators/MinIO      | `false`  |

Part size must be an integer from 5 through 5120, concurrency and retries
must be positive, path-style addressing must be a valid boolean, and a KMS key
ID is rejected unless KMS encryption is selected. S3 initialization is lazy,
so a broken AWS profile does not affect purely local or HTTP operations.

## HTTP archive sources

HTTP(S) URLs can be used with `-f` for `-t` and `-x`:

```bash
gotgz -tf https://example.com/releases/archive.tar.gz
gotgz -xvf https://example.com/releases/archive.zip -C /tmp/restore
```

HTTP is source-only: it cannot be a create destination or an extraction
target. Requests use anonymous GET through Go's default HTTP client; gotgz
does not provide a flag for custom headers or authentication.

ZIP sources use byte ranges when a reliable size and ETag/Last-Modified
validator are available. Otherwise the initial response is staged to a
temporary file. Non-local ZIP staging is limited by
`GOTGZ_ZIP_STAGING_LIMIT_BYTES`, which defaults to 1 GiB; increase it when a
large source cannot use range reads.

## Progress and exit status

Progress is written to stderr. It is automatic on an interactive terminal;
`--progress` forces it and `--no-progress` disables live updates. With
`--no-progress`, a successful non-interactive run still prints a final
`gotgz: completed in ...` line. `-v` writes member names to stdout.

Exit statuses are:

| Status | Meaning                                                                                             |
| -----: | --------------------------------------------------------------------------------------------------- |
|    `0` | Success with no warnings                                                                            |
|    `1` | Operation completed with warnings, such as metadata recovery or an unsupported optional ZIP feature |
|    `2` | Fatal parse, I/O, format, content, or path-safety failure                                           |

When sending extracted file bytes to stdout with `-O`, keep progress and
diagnostics on stderr and avoid `-v` if stdout must contain only data.

## Development

Prerequisites:

- Go 1.26.1 or newer.
- Docker and Docker Compose for the integration layer.
- `golangci-lint` for `make lint`.

Useful commands:

```bash
make build
make unit-test
make integration-test
make e2e-test
make all
```

`make test` runs the integration and CLI end-to-end layers. The integration
command also includes ordinary tests without a build tag, but `make test` does
not invoke the standalone `make unit-test` target. Run `make unit-test`
separately for a Docker-free unit gate. See [AGENTS.md](AGENTS.md) for package
boundaries, validation expectations, cache workarounds, and CI/release details.

## License

See [LICENSE](LICENSE).
