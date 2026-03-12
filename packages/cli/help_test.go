package cli

import (
	"strings"
	"testing"
)

func TestHelpTextDefaultProgram(t *testing.T) {
	got := HelpText("")
	if !strings.HasPrefix(got, "gotgz - tar-compatible archiver") {
		t.Fatalf("unexpected help header: %q", got)
	}
	if !strings.Contains(got, "gotgz -c -f <archive> [members...]") {
		t.Fatalf("default program name not rendered in usage:\n%s", got)
	}
}

func TestHelpTextCustomProgram(t *testing.T) {
	got := HelpText("mytar")
	if !strings.HasPrefix(got, "mytar - tar-compatible archiver") {
		t.Fatalf("unexpected help header: %q", got)
	}
	if !strings.Contains(got, "mytar -x -f <archive> [members...]") {
		t.Fatalf("custom program name not rendered in usage:\n%s", got)
	}
}

func TestHelpTextMentionsZipAutoDetectAndWarnings(t *testing.T) {
	got := HelpText("gotgz")
	wantContains := []string{
		"-c, --create",
		"-x, --extract",
		"-t, --list",
		"-O, --to-stdout",
		"--cd <dir|s3://...>",
		"--directory <dir|s3://...>",
		"for .zip output it maps to Deflate level",
		"create infers archive output from the archive suffix: .zip creates zip",
		"create requires explicit tar compression flags to match the archive suffix, except with -f -",
		"auto-detect archive type by magic bytes, then file extension, then content-type",
		"extract/list on .zip archives ignore tar-only compression flags and metadata owner/xattr/acl options with warnings",
		"-z, --gzip, --gunzip",
		"-j, --bzip, --bzip2",
		"-J, --xz",
		"--s3-cache-control <value>",
		"--s3-tag <key=value>",
		"--split-size <size>",
		"Split archive output into partNNNN volumes (.zip and tar-family, create mode only)",
	}
	for _, want := range wantContains {
		if !strings.Contains(got, want) {
			t.Fatalf("help text missing %q:\n%s", want, got)
		}
	}
}
