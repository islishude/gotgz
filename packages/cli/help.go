package cli

import "fmt"

func HelpText(program string) string {
	if program == "" {
		program = "gotgz"
	}
	return fmt.Sprintf(`%s - tar-compatible archiver with S3 and HTTP source support

Usage:
  %s -c -f <archive> [members...]
  %s -x -f <archive> [members...]
  %s -t -f <archive> [members...]
  %s [bundled flags] <archive> [members...]   (example: %s -cvf out.tar dir)

Modes:
  -c, --create      Create archive
  -x, --extract     Extract archive
  -t, --list        List archive contents

Main Options:
  -f <archive>      Archive path: local file, -, s3://bucket/key, S3 ARN, or http(s):// URL (source-only for -x/-t)
  -suffix <value>, --suffix <value>
                    Add suffix to archive filename in create mode (built-in date format uses 20060102 layout)
  --split-size <size>
                    Split tar-family output into partNNNN volumes (create mode only)
  -C <dir|s3://...>, --cd <dir|s3://...>, --directory <dir|s3://...>
                    Change directory before create/extract
  --s3-cache-control <value>
                    Set Cache-Control header when writing to S3 targets
  --s3-tag <key=value>
                    Add an S3 object tag when writing to S3 targets
  --strip-components <count>
                    Remove <count> leading path elements when extracting
  -v                Verbose output
  -O, --to-stdout   Extract regular file data to stdout
  --progress        Force progress output (writes to stderr)
  --no-progress     Disable progress output
  -h, --help        Show this help message

Compression:
  -z, --gzip, --gunzip
                    gzip
  -j, --bzip, --bzip2
                    bzip2
  -J, --xz          xz
  --zstd            zstd
  --lz4             lz4
  (create infers archive output from the archive suffix: .zip creates zip; tar suffixes such as .tar.gz/.tgz/.gz, .tar.bz2/.tbz2/.tbz/.bz2, .tar.xz/.txz/.xz, .tar.zst/.tzst/.zst/.zstd, and .tar.lz4/.tlz4/.lz4 select tar compression; .tar/.tape mean no compression)
  (create requires explicit tar compression flags to match the archive suffix, except with -f -)
  -compression-level <1-9>, --compression-level <1-9>
                    Compression level for create mode; for .zip output it maps to Deflate level
  (extract/list auto-detect archive type by magic bytes, then file extension, then content-type)
  (extract/list on .zip archives ignore tar-only compression flags and metadata owner/xattr/acl options with warnings)

Ownership & Permissions:
  --same-owner
  --no-same-owner
  --same-permissions
  --no-same-permissions
  --numeric-owner
  --xattrs           Archive or extract extended attributes (default: disabled)
  --acl              Archive or extract POSIX.1e/NFSv4 ACLs (default: disabled)

Exclude:
  --exclude <pattern>
  --exclude-from <file>
  --wildcards
`, program, program, program, program, program, program)
}
