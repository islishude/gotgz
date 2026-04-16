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
- **Progress + timing** — byte-based progress with ETA and elapsed time on TTY (or force with `--progress`); `--no-progress` still prints the final elapsed time
- **S3 encryption** — configurable server-side encryption (AES256, SSE-KMS)
- **S3 object tags** — repeat `--s3-tag key=value` on S3 writes

## Installation

Build from source (requires Go 1.26+):

```bash
git clone https://github.com/islishude/gotgz.git
cd gotgz
make install
```

Download pre-built binaries from the [releases page](https://github.com/islishude/gotgz/releases/latest)

Use with docker:

```bash
docker run --rm -it -v "$(pwd)":/data ghcr.io/islishude/gotgz
```

## Usage

`gotgz` follows the same CLI conventions as GNU `tar`:

### Help and version

```bash
gotgz --help
gotgz --version
gotgz -V
```

`-v` remains the tar-compatible verbose flag in bundled forms such as `-cvf` and `-xvf`.

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

# Split large archive output into independently extractable volumes
gotgz -cvf archive.tar.gz --split-size 2GiB dir1 file1.txt
gotgz -cvf archive.zip --split-size 2GiB dir1 file1.txt

# Local files → S3
gotgz -cvf s3://my-bucket/backups/archive.tar.gz dir1 file1.txt

# Local files → S3 with Cache-Control
gotgz -cvf s3://my-bucket/backups/archive.tar.gz --s3-cache-control "max-age=3600,public" dir1 file1.txt

# Local files → S3 with object tags
gotgz -cvf s3://my-bucket/backups/archive.tar.gz --s3-tag team=archive --s3-tag env=prod dir1 file1.txt

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
gotgz -xvf backup.part0001.zip -C /tmp/output

# HTTP archive → local directory
gotgz -xvf https://example.com/backups/archive.tar.gz -C /tmp/output

# HTTP zip archive → local directory
gotgz -xvf https://example.com/backups/archive.zip -C /tmp/output

# Local archive → S3
gotgz -xvf archive.tar -C s3://my-bucket/restored/

# Local archive → S3 with Cache-Control
gotgz -xvf archive.tar -C s3://my-bucket/restored/ --s3-cache-control no-store

# Local archive → S3 with object tags
gotgz -xvf archive.tar -C s3://my-bucket/restored/ --s3-tag team=restore --s3-tag env=prod
```

### List contents

```bash
gotgz -tf archive.tar.gz
gotgz -tf archive.zip
gotgz -tf backup.part0001.zip
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

Use `--split-size=<size>` in create mode to emit `.zip` or tar-family output as `partNNNN` volumes such as `archive.part0001.zip` or `archive.part0001.tar.gz`.  
Split archives are discovered automatically from `part0001` during list/extract for local files and S3 objects.
Split archives in extract mode are processed volume by volume in archive order.

When extracting or listing, archive/compression format is auto-detected by magic bytes first, then filename extension, then content type.

For extract/list on `.zip` archives, tar-specific compression flags (`-z/-j/-J/--zstd/--lz4`) and tar metadata-owner flags (`--xattrs`, `--acl`, `--same-owner`, `--numeric-owner`) are ignored with warnings. `--compression-level` still applies and maps to zip Deflate level during create.
`--split-size` supports `.zip` plus uncompressed tar and gzip/bzip2/zstd/lz4 tar output, but not xz, `-f -`, or HTTP multi-volume input.

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
Use repeatable `--s3-tag key=value` flags to add S3 object tags for archive uploads and extract targets.

### Required S3 permissions

`gotgz` only uses a small set of S3 data-plane permissions. The exact IAM policy depends on which S3 features you use:

- **Read S3 archives or S3 member objects**: `s3:GetObject`
- **Write S3 archives (`-f s3://...`) or extract to S3 (`-C s3://...`)**: `s3:PutObject`, `s3:AbortMultipartUpload`
- **Write S3 object tags with `--s3-tag`**: `s3:PutObjectTagging`
- **Open split archives from S3 (`*.part0001.zip`, `*.part0001.tar*`)**: `s3:ListBucket`

Notes:

- `gotgz` uses transfer-manager-backed `GetObject` for full-object S3 reads, which may issue `HeadObject` plus concurrent ranged `GetObject` requests. Explicit archive range reads still use `GetObject` with a `Range` header, and metadata/progress checks use `HeadObject`. In IAM, `HeadObject` is covered by `s3:GetObject`; there is no separate `s3:HeadObject` action.
- Large or streaming S3 writes may use multipart upload. For these uploads, S3 still maps create/upload/complete calls to `s3:PutObject`, and failed uploads are cleaned up with `s3:AbortMultipartUpload`.
- `s3:ListBucket` is only needed when `gotgz` must discover sibling split volumes under the same prefix.
- If you use SSE-KMS (`GOTGZ_S3_SSE=aws:kms`) or the bucket enforces a customer-managed KMS key, you also need KMS permissions on that key, typically `kms:Decrypt` and `kms:GenerateDataKey`.

Example bucket policy for a bucket-based read/write workflow:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "GotgzListSplitArchives",
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::my-bucket"
    },
    {
      "Sid": "GotgzReadWriteObjects",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:AbortMultipartUpload"],
      "Resource": "arn:aws:s3:::my-bucket/*"
    }
  ]
}
```

If your workflow is read-only or write-only, remove the actions you do not need. If you use `--s3-tag`, add `s3:PutObjectTagging` to the object actions. For access point ARNs, the same actions apply, but the IAM `Resource` values must use the corresponding access-point object ARNs.

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

# Split local or S3 archive output into 512 MiB volumes
gotgz -cvf archive.tar --split-size 512MiB dir/
gotgz -cvzf s3://my-bucket/backups/archive.tar.gz --split-size 512MiB dir/
gotgz -cvf archive.zip --split-size 512MiB dir/

# Wildcard member filtering for list/extract
gotgz -tf archive.tar --wildcards 'src/*.go'

# Set Cache-Control for S3 writes
gotgz -cvf s3://my-bucket/out.tar --s3-cache-control "max-age=600,public" dir/
gotgz -xvf archive.tar -C s3://my-bucket/restored/ --s3-cache-control no-cache

# Set S3 object tags for S3 writes
gotgz -cvf s3://my-bucket/out.tar --s3-tag team=archive --s3-tag env=prod dir/
gotgz -xvf archive.tar -C s3://my-bucket/restored/ --s3-tag team=restore

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
gotgz -cvf out.tar --no-progress dir/   # hides live updates but still prints "completed in ..."
```

## Environment Variables

| Variable                        | Description                                                                           | Default      |
| ------------------------------- | ------------------------------------------------------------------------------------- | ------------ |
| `GOTGZ_S3_SSE`                  | Server-side encryption type (`AES256`, `aws:kms`, `none`)                             | `AES256`     |
| `GOTGZ_S3_SSE_KMS_KEY_ID`       | KMS key ID for SSE-KMS encryption                                                     |              |
| `GOTGZ_S3_PART_SIZE_MB`         | S3 transfer part size in MB for multipart uploads and transfer-manager downloads      | `16`         |
| `GOTGZ_S3_CONCURRENCY`          | S3 transfer concurrency for multipart uploads and transfer-manager downloads          | `4`          |
| `GOTGZ_S3_MAX_RETRIES`          | Maximum retry attempts for S3 operations                                              |              |
| `GOTGZ_S3_USE_PATH_STYLE`       | Use path-style S3 addressing (for RustStack/MinIO)                                    | `false`      |
| `GOTGZ_ZIP_STAGING_LIMIT_BYTES` | Max bytes spooled for non-local ZIP list/extract staging (`-`, `s3://`, `http(s)://`) | `1073741824` |

Standard AWS SDK environment variables (`AWS_REGION`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_ENDPOINT_URL`, etc.) are also respected.

## Development

### Prerequisites

- Go 1.26+
- Docker & Docker Compose (for `make integration-test`)

### Run unit tests

```bash
make unit-test
```

### Run integration tests

The repository now uses three explicit test layers:

- `make unit-test` runs the default untagged unit suite and refreshes `coverage.txt`.
- `make integration-test` starts the local S3 emulator with Docker Compose, sets `GOTGZ_TEST_S3_ENDPOINT=http://localhost:4566`, and runs `go test -tags=integration ./...`.
- `make e2e-test` runs the tagged CLI subprocess suite with `go test -tags=e2e ./cmd/gotgz`.
- `make test` runs all three layers in order.

If you want to invoke the tagged layers directly:

```bash
go test ./...
GOTGZ_TEST_S3_ENDPOINT=http://localhost:4566 go test -v -tags=integration ./...
go test -v -tags=e2e ./cmd/gotgz
```

`packages/engine` is the package that owns the default engine unit tests and the tagged `integration` collaboration tests; the older `./internal/engine/` path is no longer used.

The integration layer creates all fixtures dynamically with `t.TempDir()` plus temporary S3 buckets. The CLI end-to-end layer lives under `cmd/gotgz` and exercises real subprocess flows.

## License

See [LICENSE](LICENSE) for details.
