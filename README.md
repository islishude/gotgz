# gotgz

A Linux `tar`-compatible CLI tool written in Go, with native AWS S3 support as both archive source and destination, plus HTTP(S) archive source support for extract/list.

## Features

- **Drop-in tar replacement** — supports common `tar` flags (`-c`, `-x`, `-t`, `-v`, `-f`, `-C`, `-O`) and long forms such as `--create`, `--extract`, `--list`, `--cd`/`--directory`, `--to-stdout`
- **AWS S3 integration** — use `s3://bucket/key` URIs or S3 ARNs directly in `-f` and member arguments
- **HTTP archive source** — use `http://` or `https://` URLs directly in `-f` for list/extract
- **Multiple archive/compression formats** — native `.zip` plus tar-family compression: gzip (`-z`/`--gzip`/`--gunzip`), bzip2 (`-j`/`--bzip`/`--bzip2`), xz (`-J`/`--xz`), zstd (`--zstd`), lz4 (`--lz4`), with auto-detection on create/extract/list
- **PAX format** — preserves metadata on demand: `--xattrs` for extended attributes, `--acl` for ACLs
- **Permission control** — `--same-owner`, `--same-permissions` (`--numeric-owner` accepted for tar compatibility)
- **Exclude patterns** — `--exclude` and `--exclude-from` (glob matching)
- **Member filtering on extract/list** — explicit member names, optionally with `--wildcards`
- **Path stripping on extract** — `--strip-components <count>` removes leading path segments
- **Progress + timing** — byte-based progress with ETA and elapsed time on TTY (or force with `--progress`)
- **S3 encryption** — configurable server-side encryption (AES256, SSE-KMS)

## Installation

```bash
go install github.com/islishude/gotgz/cmd/gotgz@latest
```

Or build from source:

```bash
git clone https://github.com/islishude/gotgz.git
cd gotgz
go build -o gotgz ./cmd/gotgz
```

## Usage

`gotgz` follows the same CLI conventions as GNU `tar`:

### Create an archive

```bash
# Local files → local archive
gotgz -cvf archive.tar dir1 file1.txt

# Local files → zip archive (format inferred from filename)
gotgz -cvf archive.zip dir1 file1.txt

# Local files → compressed archive (compression inferred from filename)
gotgz -cvf archive.tar.gz dir1 file1.txt
gotgz -cvf archive.tar.zst dir1 file1.txt
# or explicitly with compression flags
gotgz -cvzf archive.tar.gz dir1 file1.txt
gotgz --zstd -cvf archive.tar.zst dir1 file1.txt

# Add suffix to generated archive filename, date format is built-in and it uses `20060102` as the layout
# You can also specify a custom suffix with `-suffix` flag, for example `-suffix backup` will generate `archive-backup.tar.gz`
gotgz -cvf archive.tar.gz -suffix date dir1 file1.txt

# Split large tar-family output into independently extractable volumes
gotgz -cvf archive.tar.gz --split-size 2GiB dir1 file1.txt

# Local files → S3
gotgz -cvf s3://my-bucket/backups/archive.tar.gz dir1 file1.txt

# Local files → S3 with Cache-Control
gotgz -cvf s3://my-bucket/backups/archive.tar.gz --s3-cache-control "max-age=3600,public" dir1 file1.txt

# S3 objects → local archive
gotgz -cvf archive.tar s3://my-bucket/data/file1.txt s3://my-bucket/data/file2.txt

# S3 objects → S3 archive
gotgz -cvf s3://my-bucket/out.tar s3://my-bucket/data/file1.txt

# Local files → S3 zip archive
gotgz -cvf s3://my-bucket/out.zip dir1 file1.txt
```

### Extract an archive

```bash
# Local archive → local directory
gotgz -xvf archive.tar.gz -C /tmp/output

# Local zip archive → local directory
gotgz -xvf archive.zip -C /tmp/output

# S3 archive → local directory
gotgz -xvf s3://my-bucket/backups/archive.tar.gz -C /tmp/output

# Split archive → local directory (pass the first volume only)
gotgz -xvf backup.part0001.tar.gz -C /tmp/output

# HTTP archive → local directory
gotgz -xvf https://example.com/backups/archive.tar.gz -C /tmp/output

# HTTP zip archive → local directory
gotgz -xvf https://example.com/backups/archive.zip -C /tmp/output

# Local archive → S3
gotgz -xvf archive.tar -C s3://my-bucket/restored/

# Local archive → S3 with Cache-Control
gotgz -xvf archive.tar -C s3://my-bucket/restored/ --s3-cache-control no-store
```

### List contents

```bash
gotgz -tf archive.tar.gz
gotgz -tf archive.zip
gotgz -tf s3://my-bucket/backups/archive.tar.gz
gotgz -tf https://example.com/backups/archive.tar.gz
```

### Compression options

| Flag                       | Format |
| -------------------------- | ------ |
| `-z`, `--gzip`, `--gunzip` | gzip   |
| `-j`, `--bzip`, `--bzip2`  | bzip2  |
| `-J`, `--xz`               | xz     |
| `--zstd`                   | zstd   |
| `--lz4`                    | lz4    |

You can control compression strength for create mode with `-compression-level=<1-9>` (or `--compression-level=<1-9>`).  
If not provided, each algorithm uses its own default level.

In create mode, archive output is inferred from the archive name. `.zip` creates a zip archive.  
For tar-family output, compression is inferred from the archive name when you omit `-z/-j/-J/--zstd/--lz4`.  
Supported suffixes are `.tar.gz/.tgz/.gz`, `.tar.bz2/.tbz2/.tbz/.bz2`, `.tar.xz/.txz/.xz`, `.tar.zst/.tzst/.zst/.zstd`, and `.tar.lz4/.tlz4/.lz4`.  
`.tar` and `.tape` mean uncompressed tar, and unknown suffixes default to uncompressed tar.  
If you do pass an explicit tar-family compression flag in create mode, it must match the archive suffix. The only exception is `-f -`, because stdout has no filename.

Use `--split-size=<size>` in create mode to emit tar-family output as `partNNNN` volumes such as `archive.part0001.tar.gz`.  
Split archives are discovered automatically from `part0001` during list/extract for local files and S3 objects.

When extracting or listing, archive/compression format is auto-detected by magic bytes first, then filename extension, then content type.

For extract/list on `.zip` archives, tar-specific compression flags (`-z/-j/-J/--zstd/--lz4`) and tar metadata-owner flags (`--xattrs`, `--acl`, `--same-owner`, `--numeric-owner`) are ignored with warnings. `--compression-level` still applies and maps to zip Deflate level during create.
`--split-size` currently supports uncompressed tar plus gzip/zstd/lz4 output, but not zip, bzip2, xz, `-f -`, or HTTP multi-volume input.

### S3 addressing

Both S3 URI and ARN forms are supported:

```bash
# S3 URI
gotgz -tf s3://my-bucket/path/to/archive.tar

# S3 object ARN
gotgz -tf arn:aws:s3:::my-bucket/path/to/archive.tar

# S3 Access Point ARN
gotgz -tf arn:aws:s3:us-west-2:123456789012:accesspoint/myap/object/path/to/archive.tar

# S3 object as member argument
gotgz -cvf archive.tar s3://my-bucket/path/to/file.txt

# Add custom S3 object metadata via query string when uploading archives
gotgz -cvzf "s3://my-bucket/backups/archive.tgz?env=prod&owner=platform" dir/
```

Use `--s3-cache-control` to set the S3 `Cache-Control` header for archive uploads (`-f s3://...`) and extract targets (`-C s3://...`) without URL-encoding.

### HTTP archive source

`http://` and `https://` archive URLs are supported as `-f` sources for:

- `-x` extract
- `-t` list

Current limitation: HTTP URLs are source-only in this release. Create mode (`-c`) does not support HTTP targets, and HTTP requests use anonymous GET without custom headers/auth.

### Additional options

```bash
# Extract to stdout
gotgz -xOf archive.tar path/to/file.txt

# Exclude patterns
gotgz -cvf archive.tar --exclude='*.log' --exclude-from=excludes.txt dir/

# Split local or S3 tar output into 512 MiB volumes
gotgz -cvf archive.tar --split-size 512MiB dir/
gotgz -cvzf s3://my-bucket/backups/archive.tar.gz --split-size 512MiB dir/

# Wildcard member filtering for list/extract
gotgz -tf archive.tar --wildcards 'src/*.go'

# Set Cache-Control for S3 writes
gotgz -cvf s3://my-bucket/out.tar --s3-cache-control "max-age=600,public" dir/
gotgz -xvf archive.tar -C s3://my-bucket/restored/ --s3-cache-control no-cache

# Permission preservation
gotgz -xvf archive.tar --same-owner --same-permissions

# Explicitly enable ACL archive/extract
gotgz -cvf archive.tar --acl dir/
gotgz -xvf archive.tar --acl -C /tmp/output

# Explicitly enable xattrs archive/extract
gotgz -cvf archive.tar --xattrs dir/
gotgz -xvf archive.tar --xattrs -C /tmp/output

# Parsed for tar compatibility (currently no behavior change)
gotgz -xvf archive.tar --numeric-owner

# Strip leading path components while extracting
gotgz -xvf archive.tar --strip-components=1 -C /tmp/output

# Legacy (bundled) syntax
gotgz cvf archive.tar dir/

# Progress behavior
gotgz -xvf archive.tar --progress
gotgz -cvf out.tar --no-progress dir/
```

## Environment Variables

| Variable                        | Description                                                                           | Default      |
| ------------------------------- | ------------------------------------------------------------------------------------- | ------------ |
| `GOTGZ_S3_SSE`                  | Server-side encryption type (`AES256`, `aws:kms`, `none`)                             | `AES256`     |
| `GOTGZ_S3_SSE_KMS_KEY_ID`       | KMS key ID for SSE-KMS encryption                                                     |              |
| `GOTGZ_S3_PART_SIZE_MB`         | Multipart upload part size in MB                                                      | `16`         |
| `GOTGZ_S3_CONCURRENCY`          | Multipart upload concurrency                                                          | `4`          |
| `GOTGZ_S3_MAX_RETRIES`          | Maximum retry attempts for S3 operations                                              |              |
| `GOTGZ_S3_USE_PATH_STYLE`       | Use path-style S3 addressing (for RustStack/MinIO)                                    | `false`      |
| `GOTGZ_ZIP_STAGING_LIMIT_BYTES` | Max bytes spooled for non-local ZIP list/extract staging (`-`, `s3://`, `http(s)://`) | `1073741824` |

Standard AWS SDK environment variables (`AWS_REGION`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_ENDPOINT_URL`, etc.) are also respected.

## Development

### Prerequisites

- Go 1.26+
- Docker & Docker Compose (for integration tests)

### Run unit tests

```bash
go test ./...
```

### Run integration tests (with RustStack)

The integration tests require a running RustStack S3 endpoint to test S3 operations:

```bash
# Start RustStack
docker compose up -d --wait

# Run integration tests
GOTGZ_TEST_S3_ENDPOINT=http://localhost:4566 go test -v -run TestS3 ./internal/engine/ -count=1

# Tear down
docker compose down
```

## License

See [LICENSE](LICENSE) for details.
