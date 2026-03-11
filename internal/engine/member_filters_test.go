package engine

import (
	"testing"

	"github.com/islishude/gotgz/internal/cli"
)

func TestShouldSkipMemberWithMatcher(t *testing.T) {
	t.Run("no members keeps everything", func(t *testing.T) {
		if shouldSkipMemberWithMatcher(nil, "a.txt") {
			t.Fatal("nil matcher should not skip members")
		}
	})

	t.Run("exact matcher treats glob text literally", func(t *testing.T) {
		matcher := newMemberMatcher(cli.Options{Members: []string{"logs/*.txt"}})
		if shouldSkipMemberWithMatcher(matcher, "logs/*.txt") {
			t.Fatal("literal member should match exactly")
		}
		if !shouldSkipMemberWithMatcher(matcher, "logs/app.txt") {
			t.Fatal("literal member should not behave like a glob")
		}
	})

	t.Run("wildcard matcher uses exact set and globs", func(t *testing.T) {
		matcher := newMemberMatcher(cli.Options{
			Members:   []string{"README.md", "logs/*.txt", "["},
			Wildcards: true,
		})
		if shouldSkipMemberWithMatcher(matcher, "README.md") {
			t.Fatal("exact wildcard member should match")
		}
		if shouldSkipMemberWithMatcher(matcher, "logs/app.txt") {
			t.Fatal("glob wildcard member should match")
		}
		if !shouldSkipMemberWithMatcher(matcher, "logs/app.log") {
			t.Fatal("non-matching member should be skipped")
		}
		if !shouldSkipMemberWithMatcher(matcher, "anything") {
			t.Fatal("invalid wildcard pattern should be ignored")
		}
	})
}

func TestMatchExcludeWithMatcher(t *testing.T) {
	matcher := newCompiledPathMatcher([]string{"build/output.bin", "*.tmp"})
	if !matchExcludeWithMatcher(matcher, "build/output.bin") {
		t.Fatal("exact exclude should match")
	}
	if !matchExcludeWithMatcher(matcher, "cache.tmp") {
		t.Fatal("glob exclude should match")
	}
	if matchExcludeWithMatcher(matcher, "cache.txt") {
		t.Fatal("non-matching path should not be excluded")
	}
}
