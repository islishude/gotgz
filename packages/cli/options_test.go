package cli

import (
	"os"
	"reflect"
	"strings"
	"testing"
)

func TestParseShortBundle(t *testing.T) {
	opts, err := Parse([]string{"-cvf", "out.tar", "a", "b"})
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if opts.Mode != ModeCreate {
		t.Fatalf("mode = %q", opts.Mode)
	}
	if opts.Archive != "out.tar" {
		t.Fatalf("archive = %q", opts.Archive)
	}
	if !opts.Verbose {
		t.Fatalf("verbose expected true")
	}
	if len(opts.Members) != 2 {
		t.Fatalf("members len = %d", len(opts.Members))
	}
}

func TestParseNoArgs(t *testing.T) {
	_, err := Parse(nil)
	if err == nil {
		t.Fatalf("expected parse error")
	}
	if !strings.Contains(err.Error(), "no operation mode specified") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestParseLegacyToken(t *testing.T) {
	opts, err := Parse([]string{"cvf", "out.tar", "dir"})
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if opts.Mode != ModeCreate || opts.Archive != "out.tar" {
		t.Fatalf("unexpected parse result: %+v", opts)
	}
}

func TestParseLegacyTokenRejectsUnknownRune(t *testing.T) {
	if legacyToken("cxq") {
		t.Fatalf("legacyToken() should reject unknown flag token")
	}
}

func TestParseModeConflict(t *testing.T) {
	_, err := Parse([]string{"-cxf", "out.tar", "dir"})
	if err == nil {
		t.Fatalf("expected conflict error")
	}
}

func TestParseModeConflictOnShortC(t *testing.T) {
	_, err := Parse([]string{"-xcf", "in.tar"})
	if err == nil {
		t.Fatalf("expected conflict error")
	}
}

func TestParseModeConflictOnShortT(t *testing.T) {
	_, err := Parse([]string{"-xtf", "in.tar"})
	if err == nil {
		t.Fatalf("expected conflict error")
	}
}

func TestParseModeConflictOnLongModes(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{
			name: "extract after create",
			args: []string{"--create", "--extract", "-f", "in.tar", "a"},
		},
		{
			name: "create after extract",
			args: []string{"--extract", "--create", "-f", "in.tar", "a"},
		},
		{
			name: "list after extract",
			args: []string{"--extract", "--list", "-f", "in.tar"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.args)
			if err == nil {
				t.Fatalf("expected conflict error")
			}
		})
	}
}

func TestParseListModeShortFlag(t *testing.T) {
	opts, err := Parse([]string{"-tf", "in.tar"})
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if opts.Mode != ModeList {
		t.Fatalf("mode = %q, want %q", opts.Mode, ModeList)
	}
}

func TestParseToStdoutShortFlag(t *testing.T) {
	opts, err := Parse([]string{"-xOf", "in.tar"})
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if !opts.ToStdout {
		t.Fatalf("to-stdout expected true")
	}
}

func TestParseToStdoutLongFlag(t *testing.T) {
	opts, err := Parse([]string{"-x", "-f", "in.tar", "--to-stdout"})
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if !opts.ToStdout {
		t.Fatalf("to-stdout expected true")
	}
}

func TestParseShortCompressionFlags(t *testing.T) {
	tests := []struct {
		name string
		arg  string
		want CompressionHint
	}{
		{name: "gzip short flag", arg: "-xzf", want: CompressionGzip},
		{name: "bzip2 short flag", arg: "-xjf", want: CompressionBzip2},
		{name: "xz short flag", arg: "-xJf", want: CompressionXz},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts, err := Parse([]string{tt.arg, "in.tar"})
			if err != nil {
				t.Fatalf("Parse() error = %v", err)
			}
			if opts.Compression != tt.want {
				t.Fatalf("compression = %q, want %q", opts.Compression, tt.want)
			}
		})
	}
}

func TestParseLongOptions(t *testing.T) {
	opts, err := Parse([]string{
		"-x", "-f", "in.tar", "--exclude=*.tmp", "--exclude-from", "ex.txt", "--wildcards", "--numeric-owner", "--no-same-owner", "--same-permissions", "--lz4", "--strip-components=1", "--compression-level=9", "--suffix=custom", "--s3-cache-control=max-age=3600,public", "--s3-tag=team=archive", "--acl", "--xattrs", "--progress",
	})
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if !opts.Wildcards || !opts.NumericOwner {
		t.Fatalf("expected wildcard and numeric-owner true")
	}
	if opts.SameOwner == nil || *opts.SameOwner {
		t.Fatalf("expected no-same-owner")
	}
	if opts.SamePermissions == nil || !*opts.SamePermissions {
		t.Fatalf("expected same-permissions")
	}
	if len(opts.Exclude) != 1 || len(opts.ExcludeFrom) != 1 {
		t.Fatalf("exclude flags not parsed")
	}
	if opts.Compression != CompressionLz4 {
		t.Fatalf("compression = %q, want %q", opts.Compression, CompressionLz4)
	}
	if opts.StripComponents != 1 {
		t.Fatalf("strip-components = %d, want 1", opts.StripComponents)
	}
	if opts.CompressionLevel == nil || *opts.CompressionLevel != 9 {
		t.Fatalf("compression-level = %v, want 9", opts.CompressionLevel)
	}
	if opts.Suffix != "custom" {
		t.Fatalf("suffix = %q, want %q", opts.Suffix, "custom")
	}
	if opts.S3CacheControl != "max-age=3600,public" {
		t.Fatalf("s3-cache-control = %q, want %q", opts.S3CacheControl, "max-age=3600,public")
	}
	if !reflect.DeepEqual(opts.S3ObjectTags, map[string]string{"team": "archive"}) {
		t.Fatalf("s3 object tags = %#v", opts.S3ObjectTags)
	}
	if !opts.ACL {
		t.Fatalf("acl expected true")
	}
	if !opts.Xattrs {
		t.Fatalf("xattrs expected true")
	}
	if opts.Progress != ProgressAlways {
		t.Fatalf("progress = %q, want %q", opts.Progress, ProgressAlways)
	}
}

func TestParseSplitSize(t *testing.T) {
	tests := []struct {
		name string
		arg  string
		want int64
	}{
		{name: "bytes", arg: "2048", want: 2048},
		{name: "short unit", arg: "2K", want: 2 * 1024},
		{name: "binary unit", arg: "3MiB", want: 3 * 1024 * 1024},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts, err := Parse([]string{"-c", "-f", "out.tar", "-split-size", tt.arg, "dir"})
			if err != nil {
				t.Fatalf("Parse() error = %v", err)
			}
			if opts.SplitSizeBytes != tt.want {
				t.Fatalf("split-size = %d, want %d", opts.SplitSizeBytes, tt.want)
			}
		})
	}
}

func TestParseCreateCompressionFromArchiveName(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want CompressionHint
	}{
		{name: "gzip from tar.gz", args: []string{"-c", "-f", "out.tar.gz", "dir"}, want: CompressionGzip},
		{name: "gzip from tgz", args: []string{"-c", "-f", "out.tgz", "dir"}, want: CompressionGzip},
		{name: "zstd from tar.zst", args: []string{"-c", "-f", "out.tar.zst", "dir"}, want: CompressionZstd},
		{name: "lz4 from tar.lz4", args: []string{"-c", "-f", "out.tar.lz4", "dir"}, want: CompressionLz4},
		{name: "none from tar", args: []string{"-c", "-f", "out.tar", "dir"}, want: CompressionNone},
		{name: "none from unknown suffix", args: []string{"-c", "-f", "out.bin", "dir"}, want: CompressionNone},
		{name: "none from stdio", args: []string{"-c", "-f", "-", "dir"}, want: CompressionNone},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts, err := Parse(tt.args)
			if err != nil {
				t.Fatalf("Parse() error = %v", err)
			}
			if opts.Compression != tt.want {
				t.Fatalf("compression = %q, want %q", opts.Compression, tt.want)
			}
		})
	}
}

func TestParseCreateCompressionRequiresMatchingArchiveName(t *testing.T) {
	tests := []struct {
		name   string
		args   []string
		errSub string
	}{
		{
			name:   "tar implies none",
			args:   []string{"-c", "-f", "out.tar", "--gzip", "dir"},
			errSub: `compression "gzip" does not match archive name "out.tar" (implies no compression)`,
		},
		{
			name:   "unknown suffix",
			args:   []string{"-c", "-f", "out.bin", "--gzip", "dir"},
			errSub: `compression "gzip" does not match archive name "out.bin" (implies no recognized compression suffix)`,
		},
		{
			name:   "zip archive",
			args:   []string{"-c", "-f", "out.zip", "--gzip", "dir"},
			errSub: `compression "gzip" does not match archive name "out.zip" (implies zip archive format)`,
		},
		{
			name:   "different compressed suffix",
			args:   []string{"-c", "-f", "out.tar.xz", "--gzip", "dir"},
			errSub: `compression "gzip" does not match archive name "out.tar.xz" (implies "xz" compression)`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.args)
			if err == nil {
				t.Fatalf("expected parse error")
			}
			if !strings.Contains(err.Error(), tt.errSub) {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestParseCreateCompressionAllowsStdoutWithoutName(t *testing.T) {
	opts, err := Parse([]string{"-c", "-f", "-", "--gzip", "dir"})
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if opts.Compression != CompressionGzip {
		t.Fatalf("compression = %q, want %q", opts.Compression, CompressionGzip)
	}
}

func TestParseLongModeAliases(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want Mode
	}{
		{name: "create", args: []string{"--create", "-f", "out.tar", "a"}, want: ModeCreate},
		{name: "extract", args: []string{"--extract", "-f", "in.tar"}, want: ModeExtract},
		{name: "list", args: []string{"--list", "-f", "in.tar"}, want: ModeList},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts, err := Parse(tt.args)
			if err != nil {
				t.Fatalf("Parse() error = %v", err)
			}
			if opts.Mode != tt.want {
				t.Fatalf("mode = %q, want %q", opts.Mode, tt.want)
			}
		})
	}
}

func TestParseDirectoryLongAliases(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "directory long option",
			args: []string{"-x", "-f", "in.tar", "--directory", "/tmp/output"},
			want: "/tmp/output",
		},
		{
			name: "cd long option",
			args: []string{"-x", "-f", "in.tar", "--cd=s3://bucket/path"},
			want: "s3://bucket/path",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts, err := Parse(tt.args)
			if err != nil {
				t.Fatalf("Parse() error = %v", err)
			}
			if opts.Chdir != tt.want {
				t.Fatalf("chdir = %q, want %q", opts.Chdir, tt.want)
			}
		})
	}
}

func TestParseDoubleDashMembers(t *testing.T) {
	opts, err := Parse([]string{"-xf", "in.tar", "--", "--literal", "path/file"})
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if len(opts.Members) != 2 {
		t.Fatalf("members len = %d, want 2", len(opts.Members))
	}
	if opts.Members[0] != "--literal" || opts.Members[1] != "path/file" {
		t.Fatalf("members = %#v", opts.Members)
	}
}

func TestParseLongOptionsAdditionalCoverage(t *testing.T) {
	opts, err := Parse([]string{"-x", "-f", "in.tar", "--same-owner", "--no-same-permissions", "--zstd"})
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if opts.SameOwner == nil || !*opts.SameOwner {
		t.Fatalf("expected same-owner")
	}
	if opts.SamePermissions == nil || *opts.SamePermissions {
		t.Fatalf("expected no-same-permissions")
	}
	if opts.Compression != CompressionZstd {
		t.Fatalf("compression = %q, want %q", opts.Compression, CompressionZstd)
	}
}

func TestParseSingleDashCompatibilityErrors(t *testing.T) {
	tests := []struct {
		name   string
		arg    string
		errSub string
	}{
		{
			name:   "unsupported single-dash compression option",
			arg:    "-compression-levelx=7",
			errSub: "unsupported option -compression-levelx=7",
		},
		{
			name:   "single-dash compression option missing value",
			arg:    "-compression-level",
			errSub: "option --compression-level requires a value",
		},
		{
			name:   "single-dash compression option invalid value",
			arg:    "-compression-level=0",
			errSub: "option --compression-level requires an integer between 1 and 9",
		},
		{
			name:   "unsupported single-dash suffix option",
			arg:    "-suffixx=date",
			errSub: "unsupported option -suffixx=date",
		},
		{
			name:   "single-dash suffix missing value",
			arg:    "-suffix",
			errSub: "option --suffix requires a value",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse([]string{"-x", "-f", "in.tar", tt.arg})
			if err == nil {
				t.Fatalf("expected parse error")
			}
			if !strings.Contains(err.Error(), tt.errSub) {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

// TestParseLongCompressionAliases verifies long-form aliases map to expected compression hints.
func TestParseLongCompressionAliases(t *testing.T) {
	tests := []struct {
		name string
		arg  string
		want CompressionHint
	}{
		{name: "gzip alias", arg: "--gzip", want: CompressionGzip},
		{name: "gunzip alias", arg: "--gunzip", want: CompressionGzip},
		{name: "bzip alias", arg: "--bzip", want: CompressionBzip2},
		{name: "bzip2 alias", arg: "--bzip2", want: CompressionBzip2},
		{name: "xz alias", arg: "--xz", want: CompressionXz},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts, err := Parse([]string{"-x", "-f", "in.tar", tt.arg})
			if err != nil {
				t.Fatalf("Parse() error = %v", err)
			}
			if opts.Compression != tt.want {
				t.Fatalf("compression = %q, want %q", opts.Compression, tt.want)
			}
		})
	}
}

func TestParseLongOptionsMissingValues(t *testing.T) {
	tests := []struct {
		name   string
		arg    string
		errSub string
	}{
		{name: "exclude missing value", arg: "--exclude", errSub: "option --exclude requires a value"},
		{name: "exclude-from missing value", arg: "--exclude-from", errSub: "option --exclude-from requires a value"},
		{name: "strip-components missing value", arg: "--strip-components", errSub: "option --strip-components requires a value"},
		{name: "compression-level missing value", arg: "--compression-level", errSub: "option --compression-level requires a value"},
		{name: "split-size missing value", arg: "--split-size", errSub: "option --split-size requires a value"},
		{name: "suffix missing value", arg: "--suffix", errSub: "option --suffix requires a value"},
		{name: "directory missing value", arg: "--directory", errSub: "option --directory requires a value"},
		{name: "cd missing value", arg: "--cd", errSub: "option --cd requires a value"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse([]string{"-x", "-f", "in.tar", tt.arg})
			if err == nil {
				t.Fatalf("expected parse error")
			}
			if !strings.Contains(err.Error(), tt.errSub) {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestParseSplitSizeValidation(t *testing.T) {
	tests := []struct {
		name   string
		args   []string
		errSub string
	}{
		{
			name:   "invalid size",
			args:   []string{"-c", "-f", "out.tar", "--split-size=0", "dir"},
			errSub: "option --split-size requires a positive byte size",
		},
		{
			name:   "extract mode unsupported",
			args:   []string{"-x", "-f", "in.tar", "--split-size=1M"},
			errSub: "option --split-size is only supported in create mode",
		},
		{
			name:   "stdio unsupported",
			args:   []string{"-c", "-f", "-", "--split-size=1M", "dir"},
			errSub: "option --split-size does not support -f -",
		},
		{
			name:   "zip unsupported",
			args:   []string{"-c", "-f", "out.zip", "--split-size=1M", "dir"},
			errSub: "option --split-size does not support zip archives",
		},
		{
			name:   "bzip2 unsupported",
			args:   []string{"-c", "-f", "out.tar.bz2", "--split-size=1M", "--bzip2", "dir"},
			errSub: "option --split-size does not support bzip2 compression",
		},
		{
			name:   "bzip2 inferred from archive name",
			args:   []string{"-c", "-f", "out.tar.bz2", "--split-size=1M", "dir"},
			errSub: "option --split-size does not support bzip2 compression",
		},
		{
			name:   "xz unsupported",
			args:   []string{"-c", "-f", "out.tar.xz", "--split-size=1M", "--xz", "dir"},
			errSub: "option --split-size does not support xz compression",
		},
		{
			name:   "xz inferred from archive name",
			args:   []string{"-c", "-f", "out.tar.xz", "--split-size=1M", "dir"},
			errSub: "option --split-size does not support xz compression",
		},
		{
			name:   "reserved archive name",
			args:   []string{"-c", "-f", "out.part0001.tar.gz", "--split-size=1M", "dir"},
			errSub: "option --split-size cannot use an archive name that already contains .partNNNN",
		},
		{
			name:   "reserved suffix lowercase",
			args:   []string{"-c", "-f", "out.tar", "--suffix=part0001", "dir"},
			errSub: "option --suffix cannot use reserved split name",
		},
		{
			name:   "reserved suffix uppercase",
			args:   []string{"-c", "-f", "out.tar", "--suffix=PART1", "dir"},
			errSub: "option --suffix cannot use reserved split name",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.args)
			if err == nil {
				t.Fatalf("expected parse error")
			}
			if !strings.Contains(err.Error(), tt.errSub) {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestParseSuffixAllowsNonReservedSplitText(t *testing.T) {
	opts, err := Parse([]string{"-c", "-f", "out.tar", "--suffix=my-part0001", "dir"})
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if opts.Suffix != "my-part0001" {
		t.Fatalf("suffix = %q, want %q", opts.Suffix, "my-part0001")
	}
}

func TestParseHelpShort(t *testing.T) {
	opts, err := Parse([]string{"-h"})
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if !opts.Help {
		t.Fatalf("expected Help=true")
	}
}

func TestParseHelpLong(t *testing.T) {
	opts, err := Parse([]string{"--help"})
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if !opts.Help {
		t.Fatalf("expected Help=true")
	}
}

func TestParseStripComponentsInvalid(t *testing.T) {
	_, err := Parse([]string{"-x", "-f", "in.tar", "--strip-components=-1"})
	if err == nil {
		t.Fatalf("expected parse error")
	}
}

func TestParseUnsupportedLongOption(t *testing.T) {
	_, err := Parse([]string{"-x", "-f", "in.tar", "--unknown-flag"})
	if err == nil {
		t.Fatalf("expected parse error")
	}
	if !strings.Contains(err.Error(), "unsupported option --unknown-flag") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestParseShortOptionInlineAndErrors(t *testing.T) {
	t.Run("inline short C sets chdir", func(t *testing.T) {
		opts, err := Parse([]string{"-xC/tmp", "-f", "in.tar"})
		if err != nil {
			t.Fatalf("Parse() error = %v", err)
		}
		if opts.Chdir != "/tmp" {
			t.Fatalf("chdir = %q, want /tmp", opts.Chdir)
		}
	})

	t.Run("short option missing argument", func(t *testing.T) {
		_, err := Parse([]string{"-xf"})
		if err == nil {
			t.Fatalf("expected parse error")
		}
		if !strings.Contains(err.Error(), "option -f requires an argument") {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("unsupported short option", func(t *testing.T) {
		_, err := Parse([]string{"-xq", "-f", "in.tar"})
		if err == nil {
			t.Fatalf("expected parse error")
		}
		if !strings.Contains(err.Error(), "unsupported option -q") {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestParseRequiredFieldsAndCreateMembers(t *testing.T) {
	t.Run("missing mode", func(t *testing.T) {
		_, err := Parse([]string{"-f", "in.tar"})
		if err == nil {
			t.Fatalf("expected parse error")
		}
		if !strings.Contains(err.Error(), "no operation mode specified") {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("missing archive", func(t *testing.T) {
		_, err := Parse([]string{"-x"})
		if err == nil {
			t.Fatalf("expected parse error")
		}
		if !strings.Contains(err.Error(), "option -f is required") {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("create without members", func(t *testing.T) {
		_, err := Parse([]string{"-cf", "out.tar"})
		if err == nil {
			t.Fatalf("expected parse error")
		}
		if !strings.Contains(err.Error(), "cowardly refusing to create an empty archive") {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestParseCompressionLevelSingleDash(t *testing.T) {
	opts, err := Parse([]string{"-x", "-f", "in.tar", "-compression-level=7"})
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if opts.CompressionLevel == nil || *opts.CompressionLevel != 7 {
		t.Fatalf("compression-level = %v, want 7", opts.CompressionLevel)
	}
}

func TestParseCompressionLevelInvalid(t *testing.T) {
	_, err := Parse([]string{"-x", "-f", "in.tar", "--compression-level=10"})
	if err == nil {
		t.Fatalf("expected parse error")
	}
}

func TestParseSuffixSingleDash(t *testing.T) {
	opts, err := Parse([]string{"-x", "-f", "in.tar", "-suffix=date"})
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if opts.Suffix != "date" {
		t.Fatalf("suffix = %q, want %q", opts.Suffix, "date")
	}
}

func TestParseS3CacheControlValueArg(t *testing.T) {
	opts, err := Parse([]string{"-x", "-f", "in.tar", "--s3-cache-control", " no-store "})
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if opts.S3CacheControl != "no-store" {
		t.Fatalf("s3-cache-control = %q, want %q", opts.S3CacheControl, "no-store")
	}
}

func TestParseS3CacheControlMissingValue(t *testing.T) {
	_, err := Parse([]string{"-x", "-f", "in.tar", "--s3-cache-control"})
	if err == nil {
		t.Fatalf("expected parse error")
	}
}

func TestParseS3TagValueArg(t *testing.T) {
	opts, err := Parse([]string{"-x", "-f", "in.tar", "--s3-tag", " team = archive "})
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if !reflect.DeepEqual(opts.S3ObjectTags, map[string]string{"team": "archive"}) {
		t.Fatalf("s3 object tags = %#v", opts.S3ObjectTags)
	}
}

func TestParseS3TagInlineRepeatedLastWins(t *testing.T) {
	opts, err := Parse([]string{"-x", "-f", "in.tar", "--s3-tag=team=archive", "--s3-tag", "trace=enabled", "--s3-tag=team=platform"})
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	want := map[string]string{"team": "platform", "trace": "enabled"}
	if !reflect.DeepEqual(opts.S3ObjectTags, want) {
		t.Fatalf("s3 object tags = %#v, want %#v", opts.S3ObjectTags, want)
	}
}

func TestParseS3TagAllowsEmptyValue(t *testing.T) {
	opts, err := Parse([]string{"-x", "-f", "in.tar", "--s3-tag=team="})
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if got := opts.S3ObjectTags["team"]; got != "" {
		t.Fatalf("s3 object tag value = %q, want empty string", got)
	}
}

func TestParseS3TagMissingValue(t *testing.T) {
	_, err := Parse([]string{"-x", "-f", "in.tar", "--s3-tag"})
	if err == nil {
		t.Fatalf("expected parse error")
	}
}

func TestParseS3TagRequiresEquals(t *testing.T) {
	_, err := Parse([]string{"-x", "-f", "in.tar", "--s3-tag", "team"})
	if err == nil || !strings.Contains(err.Error(), "key=value") {
		t.Fatalf("err = %v, want key=value error", err)
	}
}

func TestParseS3TagRequiresNonEmptyKey(t *testing.T) {
	_, err := Parse([]string{"-x", "-f", "in.tar", "--s3-tag", "=archive"})
	if err == nil || !strings.Contains(err.Error(), "non-empty key") {
		t.Fatalf("err = %v, want non-empty key error", err)
	}
}

func TestParseACLDefaultDisabled(t *testing.T) {
	opts, err := Parse([]string{"-x", "-f", "in.tar"})
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if opts.ACL {
		t.Fatalf("acl should be disabled by default")
	}
	if opts.Xattrs {
		t.Fatalf("xattrs should be disabled by default")
	}
	if opts.Progress != ProgressAuto {
		t.Fatalf("progress = %q, want %q", opts.Progress, ProgressAuto)
	}
}

func TestParseNoProgress(t *testing.T) {
	opts, err := Parse([]string{"-x", "-f", "in.tar", "--no-progress"})
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if opts.Progress != ProgressNever {
		t.Fatalf("progress = %q, want %q", opts.Progress, ProgressNever)
	}
}

func TestParseProgressLastFlagWins(t *testing.T) {
	opts, err := Parse([]string{"-x", "-f", "in.tar", "--progress", "--no-progress", "--progress"})
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if opts.Progress != ProgressAlways {
		t.Fatalf("progress = %q, want %q", opts.Progress, ProgressAlways)
	}
}

func TestResolvePermissionPolicyDefaults(t *testing.T) {
	opts := Options{NumericOwner: true}
	got := opts.ResolvePermissionPolicy()

	isRoot := os.Geteuid() == 0
	if got.SameOwner != isRoot {
		t.Fatalf("SameOwner = %v, want %v", got.SameOwner, isRoot)
	}
	if got.SamePerms != isRoot {
		t.Fatalf("SamePerms = %v, want %v", got.SamePerms, isRoot)
	}
	if !got.NumericOwner {
		t.Fatalf("NumericOwner should be true")
	}
}

func TestResolvePermissionPolicyOverrides(t *testing.T) {
	sameOwner := false
	samePerms := true
	opts := Options{
		NumericOwner:    false,
		SameOwner:       &sameOwner,
		SamePermissions: &samePerms,
	}

	got := opts.ResolvePermissionPolicy()
	if got.SameOwner {
		t.Fatalf("SameOwner should be false")
	}
	if !got.SamePerms {
		t.Fatalf("SamePerms should be true")
	}
	if got.NumericOwner {
		t.Fatalf("NumericOwner should be false")
	}
}

func TestResolveMetadataPolicy(t *testing.T) {
	got := (Options{Xattrs: true, ACL: false}).ResolveMetadataPolicy()
	if !got.Xattrs {
		t.Fatalf("Xattrs should be true")
	}
	if got.ACL {
		t.Fatalf("ACL should be false")
	}
}
