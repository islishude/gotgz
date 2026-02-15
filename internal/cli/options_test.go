package cli

import "testing"

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

func TestParseLegacyToken(t *testing.T) {
	opts, err := Parse([]string{"cvf", "out.tar", "dir"})
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if opts.Mode != ModeCreate || opts.Archive != "out.tar" {
		t.Fatalf("unexpected parse result: %+v", opts)
	}
}

func TestParseModeConflict(t *testing.T) {
	_, err := Parse([]string{"-cxf", "out.tar", "dir"})
	if err == nil {
		t.Fatalf("expected conflict error")
	}
}

func TestParseLongOptions(t *testing.T) {
	opts, err := Parse([]string{
		"-x", "-f", "in.tar", "--exclude=*.tmp", "--exclude-from", "ex.txt", "--wildcards", "--numeric-owner", "--no-same-owner", "--same-permissions", "--lz4", "--strip-components=1", "--compression-level=9", "--suffix=custom",
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
