package cli

import "fmt"

func HelpText(program string) string {
	if program == "" {
		program = "gotgz"
	}
	return fmt.Sprintf(`%s - tar-compatible archiver with S3 support

Usage:
  %s -c -f <archive> [members...]
  %s -x -f <archive> [members...]
  %s -t -f <archive> [members...]
  %s [bundled flags] <archive> [members...]   (example: %s -cvf out.tar dir)

Modes:
  -c                Create archive
  -x                Extract archive
  -t                List archive contents

Main Options:
  -f <archive>      Archive path: local file, -, s3://bucket/key, or S3 ARN
  -suffix <value>, --suffix <value>
                    Add suffix to archive filename in create mode (built-in date format uses 20060102 layout)
  -C <dir|s3://...> Change directory before create/extract
  --strip-components <count>
                    Remove <count> leading path elements when extracting
  -v                Verbose output
  -O                Extract regular file data to stdout
  -h, --help        Show this help message

Compression:
  -z                gzip
  -j                bzip2
  -J                xz
  --zstd            zstd
  --lz4             lz4
  -compression-level <1-9>, --compression-level <1-9>
                    Compression level for create mode; omitted uses algorithm defaults
  (extract/list auto-detects by magic bytes, then file extension)

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
