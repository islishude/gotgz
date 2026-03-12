package archivepath

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestShouldSkipMemberWithMatcher(t *testing.T) {
	t.Run("no members keeps everything", func(t *testing.T) {
		if ShouldSkipMemberWithMatcher(nil, "a.txt") {
			t.Fatal("nil matcher should not skip members")
		}
	})

	t.Run("exact matcher treats glob text literally", func(t *testing.T) {
		matcher := NewMemberMatcher([]string{"logs/*.txt"}, false)
		if ShouldSkipMemberWithMatcher(matcher, "logs/*.txt") {
			t.Fatal("literal member should match exactly")
		}
		if !ShouldSkipMemberWithMatcher(matcher, "logs/app.txt") {
			t.Fatal("literal member should not behave like a glob")
		}
	})

	t.Run("wildcard matcher uses exact set and globs", func(t *testing.T) {
		matcher := NewMemberMatcher([]string{"README.md", "logs/*.txt", "["}, true)
		if ShouldSkipMemberWithMatcher(matcher, "README.md") {
			t.Fatal("exact wildcard member should match")
		}
		if ShouldSkipMemberWithMatcher(matcher, "logs/app.txt") {
			t.Fatal("glob wildcard member should match")
		}
		if !ShouldSkipMemberWithMatcher(matcher, "logs/app.log") {
			t.Fatal("non-matching member should be skipped")
		}
		if !ShouldSkipMemberWithMatcher(matcher, "anything") {
			t.Fatal("invalid wildcard pattern should be ignored")
		}
	})
}

func TestMatchExcludeWithMatcher(t *testing.T) {
	matcher := NewCompiledPathMatcher([]string{"build/output.bin", "*.tmp"})
	if !MatchExcludeWithMatcher(matcher, "build/output.bin") {
		t.Fatal("exact exclude should match")
	}
	if !MatchExcludeWithMatcher(matcher, "cache.tmp") {
		t.Fatal("glob exclude should match")
	}
	if MatchExcludeWithMatcher(matcher, "cache.txt") {
		t.Fatal("non-matching path should not be excluded")
	}
}

func TestLoadExcludePatterns(t *testing.T) {
	tmpDir := t.TempDir()
	patternsFile := filepath.Join(tmpDir, "exclude.txt")
	content := strings.Join([]string{
		"# comment",
		"",
		"build/**",
		"*.tmp",
		"  logs/*.log  ",
	}, "\n")
	if err := os.WriteFile(patternsFile, []byte(content), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	got, err := LoadExcludePatterns([]string{"dist/*"}, []string{patternsFile})
	if err != nil {
		t.Fatalf("LoadExcludePatterns() error = %v", err)
	}
	want := []string{"dist/*", "build/**", "*.tmp", "logs/*.log"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("patterns = %#v, want %#v", got, want)
	}
}

func TestLoadExcludePatternsRejectsInvalidInlinePattern(t *testing.T) {
	_, err := LoadExcludePatterns([]string{"["}, nil)
	if err == nil {
		t.Fatal("expected invalid pattern error")
	}
	if !strings.Contains(err.Error(), "invalid exclude pattern") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoadExcludePatternsRejectsInvalidPatternInFile(t *testing.T) {
	tmpDir := t.TempDir()
	patternsFile := filepath.Join(tmpDir, "exclude.txt")
	if err := os.WriteFile(patternsFile, []byte("*.tmp\n[\n"), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	_, err := LoadExcludePatterns(nil, []string{patternsFile})
	if err == nil {
		t.Fatal("expected invalid pattern error")
	}
	if !strings.Contains(err.Error(), "exclude.txt:2") {
		t.Fatalf("error should include file and line: %v", err)
	}
}

func TestLoadExcludePatternsMissingFile(t *testing.T) {
	_, err := LoadExcludePatterns(nil, []string{"/path/that/does/not/exist"})
	if err == nil {
		t.Fatal("expected file read error")
	}
}

func TestMatcherConstructorsEmptyInput(t *testing.T) {
	if got := NewExactPathMatcher(nil); got != nil {
		t.Fatalf("NewExactPathMatcher(nil) = %#v, want nil", got)
	}
	if got := NewCompiledPathMatcher(nil); got != nil {
		t.Fatalf("NewCompiledPathMatcher(nil) = %#v, want nil", got)
	}
	if got := NewMemberMatcher(nil, true); got != nil {
		t.Fatalf("NewMemberMatcher(nil, true) = %#v, want nil", got)
	}
}

func TestCompiledPathMatcherNilReceiver(t *testing.T) {
	var matcher *CompiledPathMatcher
	if matcher.Matches("anything") {
		t.Fatal("nil matcher should not match")
	}
	if MatchExcludeWithMatcher(matcher, "anything") {
		t.Fatal("nil matcher should not match exclude")
	}
}
