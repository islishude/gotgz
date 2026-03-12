package archivepath

import (
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
