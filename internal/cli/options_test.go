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
		"-x", "-f", "in.tar", "--exclude=*.tmp", "--exclude-from", "ex.txt", "--wildcards", "--numeric-owner", "--no-same-owner", "--same-permissions",
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
}
