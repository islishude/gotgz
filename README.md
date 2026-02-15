# gotgz

A Linux `tar`-compatible CLI tool written in Go, with native AWS S3 support as both archive source and destination.

## Features

- **Drop-in tar replacement** — supports common `tar` flags (`-c`, `-x`, `-t`, `-v`, `-f`, `-C`, `-O`)
- **AWS S3 integration** — use `s3://bucket/key` URIs or S3 ARNs directly in `-f` and member arguments
- **Multiple compression formats** — gzip (`-z`), bzip2 (`-j`), xz (`-J`), zstd (`--zstd`), lz4 (`--lz4`), with auto-detection on extract
- **PAX format** — preserves metadata on demand: `--xattrs` for extended attributes, `--acl` for ACLs
- **Permission control** — `--same-owner`, `--same-permissions` (`--numeric-owner` accepted for tar compatibility)
- **Exclude patterns** — `--exclude` and `--exclude-from` (glob matching)
- **Member filtering on extract/list** — explicit member names, optionally with `--wildcards`
- **Path stripping on extract** — `--strip-components <count>` removes leading path segments
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

# Local files → compressed archive
gotgz -cvzf archive.tar.gz dir1 file1.txt

# Add suffix to generated archive filename, date format is built-in and it uses `20060102` as the layout
# You can also specify a custom suffix with `-suffix` flag, for example `-suffix backup` will generate `archive-backup.tar.gz`
gotgz -cvzf archive.tar.gz -suffix date dir1 file1.txt

# Local files → S3
gotgz -cvzf s3://my-bucket/backups/archive.tar.gz dir1 file1.txt

# S3 objects → local archive
gotgz -cvf archive.tar s3://my-bucket/data/file1.txt s3://my-bucket/data/file2.txt

# S3 objects → S3 archive
gotgz -cvf s3://my-bucket/out.tar s3://my-bucket/data/file1.txt
```

### Extract an archive

```bash
# Local archive → local directory
gotgz -xvf archive.tar.gz -C /tmp/output

# S3 archive → local directory
gotgz -xvf s3://my-bucket/backups/archive.tar.gz -C /tmp/output

# Local archive → S3
gotgz -xvf archive.tar -C s3://my-bucket/restored/
```

### List contents

```bash
gotgz -tf archive.tar.gz
gotgz -tf s3://my-bucket/backups/archive.tar.gz
```

### Compression options

| Flag     | Format |
| -------- | ------ |
| `-z`     | gzip   |
| `-j`     | bzip2  |
| `-J`     | xz     |
| `--zstd` | zstd   |
| `--lz4`  | lz4    |

You can control compression strength for create mode with `-compression-level=<1-9>` (or `--compression-level=<1-9>`).  
If not provided, each algorithm uses its own default level.

When extracting or listing, compression is auto-detected from file magic bytes or extension.

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

### Additional options

```bash
# Extract to stdout
gotgz -xOf archive.tar path/to/file.txt

# Exclude patterns
gotgz -cvf archive.tar --exclude='*.log' --exclude-from=excludes.txt dir/

# Wildcard member filtering for list/extract
gotgz -tf archive.tar --wildcards 'src/*.go'

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
```

## Environment Variables

| Variable                  | Description                                               | Default  |
| ------------------------- | --------------------------------------------------------- | -------- |
| `GOTGZ_S3_SSE`            | Server-side encryption type (`AES256`, `aws:kms`, `none`) | `AES256` |
| `GOTGZ_S3_SSE_KMS_KEY_ID` | KMS key ID for SSE-KMS encryption                         |          |
| `GOTGZ_S3_PART_SIZE_MB`   | Multipart upload part size in MB                          | `16`     |
| `GOTGZ_S3_CONCURRENCY`    | Multipart upload concurrency                              | `4`      |
| `GOTGZ_S3_MAX_RETRIES`    | Maximum retry attempts for S3 operations                  |          |
| `GOTGZ_S3_USE_PATH_STYLE` | Use path-style S3 addressing (for LocalStack/MinIO)       | `false`  |

Standard AWS SDK environment variables (`AWS_REGION`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_ENDPOINT_URL`, etc.) are also respected.

## Development

### Prerequisites

- Go 1.26+
- Docker & Docker Compose (for integration tests)

### Run unit tests

```bash
go test ./...
```

### Run integration tests (with LocalStack)

The integration tests require a running LocalStack instance to test S3 operations:

```bash
# Start LocalStack
docker compose up -d --wait

# Run integration tests
GOTGZ_TEST_S3_ENDPOINT=http://localhost:4566 go test -v -run TestS3 ./internal/engine/ -count=1

# Tear down
docker compose down
```

## License

See [LICENSE](LICENSE) for details.
