package engine

import (
	"fmt"
	"os"
	"path"
	"strings"

	"github.com/islishude/gotgz/packages/cli"
)

// compiledPathMatcher matches path-like names against exact strings and globs.
type compiledPathMatcher struct {
	exact map[string]struct{}
	globs []string
}

// newExactPathMatcher stores every value as an exact match.
func newExactPathMatcher(values []string) *compiledPathMatcher {
	if len(values) == 0 {
		return nil
	}
	m := &compiledPathMatcher{exact: make(map[string]struct{}, len(values))}
	for _, value := range values {
		m.exact[value] = struct{}{}
	}
	return m
}

// newCompiledPathMatcher classifies patterns so exact matches avoid path.Match.
func newCompiledPathMatcher(patterns []string) *compiledPathMatcher {
	if len(patterns) == 0 {
		return nil
	}
	m := &compiledPathMatcher{
		exact: make(map[string]struct{}),
		globs: make([]string, 0),
	}
	for _, pattern := range patterns {
		if strings.ContainsAny(pattern, "*?[") {
			m.globs = append(m.globs, pattern)
			continue
		}
		m.exact[pattern] = struct{}{}
	}
	if len(m.exact) == 0 && len(m.globs) == 0 {
		return nil
	}
	return m
}

// matches reports whether name matches at least one stored exact or glob rule.
func (m *compiledPathMatcher) matches(name string) bool {
	if m == nil {
		return false
	}
	if _, ok := m.exact[name]; ok {
		return true
	}
	for _, pattern := range m.globs {
		if ok, _ := path.Match(pattern, name); ok {
			return true
		}
	}
	return false
}

// newMemberMatcher compiles member filters once for list/extract scans.
func newMemberMatcher(opts cli.Options) *compiledPathMatcher {
	if len(opts.Members) == 0 {
		return nil
	}
	if !opts.Wildcards {
		return newExactPathMatcher(opts.Members)
	}
	return newCompiledPathMatcher(opts.Members)
}

// shouldSkipMemberWithMatcher reports whether name should be skipped by the
// already-compiled member matcher.
func shouldSkipMemberWithMatcher(matcher *compiledPathMatcher, name string) bool {
	if matcher == nil {
		return false
	}
	return !matcher.matches(name)
}

// loadExcludePatterns loads and validates exclude patterns from CLI args and files.
func loadExcludePatterns(inline []string, files []string) ([]string, error) {
	out := make([]string, 0, len(inline))
	for _, pattern := range inline {
		if _, err := path.Match(pattern, ""); err != nil {
			return nil, fmt.Errorf("invalid exclude pattern %q: %w", pattern, err)
		}
		out = append(out, pattern)
	}
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			return nil, err
		}
		lineNo := 0
		for line := range strings.SplitSeq(string(b), "\n") {
			lineNo++
			line = strings.TrimSpace(line)
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			if _, err := path.Match(line, ""); err != nil {
				return nil, fmt.Errorf("invalid exclude pattern %q in %s:%d: %w", line, f, lineNo, err)
			}
			out = append(out, line)
		}
	}
	return out, nil
}

// matchExcludeWithMatcher reports whether name matches at least one compiled
// exclude pattern.
func matchExcludeWithMatcher(matcher *compiledPathMatcher, name string) bool {
	return matcher.matches(name)
}
