package archivepath

import (
	"fmt"
	"os"
	"path"
	"strings"
)

// CompiledPathMatcher matches path-like names against exact strings and globs.
type CompiledPathMatcher struct {
	exact map[string]struct{}
	globs []string
}

// NewExactPathMatcher stores every value as an exact match.
func NewExactPathMatcher(values []string) *CompiledPathMatcher {
	if len(values) == 0 {
		return nil
	}
	m := &CompiledPathMatcher{exact: make(map[string]struct{}, len(values))}
	for _, value := range values {
		m.exact[value] = struct{}{}
	}
	return m
}

// NewCompiledPathMatcher classifies patterns so exact matches avoid path.Match.
func NewCompiledPathMatcher(patterns []string) *CompiledPathMatcher {
	if len(patterns) == 0 {
		return nil
	}
	m := &CompiledPathMatcher{
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

// Matches reports whether name matches at least one stored exact or glob rule.
func (m *CompiledPathMatcher) Matches(name string) bool {
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

// NewMemberMatcher compiles member filters once for list/extract scans.
func NewMemberMatcher(members []string, wildcards bool) *CompiledPathMatcher {
	if len(members) == 0 {
		return nil
	}
	if !wildcards {
		return NewExactPathMatcher(members)
	}
	return NewCompiledPathMatcher(members)
}

// ShouldSkipMemberWithMatcher reports whether name should be skipped by the
// already-compiled member matcher.
func ShouldSkipMemberWithMatcher(matcher *CompiledPathMatcher, name string) bool {
	if matcher == nil {
		return false
	}
	return !matcher.Matches(name)
}

// LoadExcludePatterns loads and validates exclude patterns from CLI args and files.
func LoadExcludePatterns(inline []string, files []string) ([]string, error) {
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

// MatchExcludeWithMatcher reports whether name matches at least one compiled
// exclude pattern.
func MatchExcludeWithMatcher(matcher *CompiledPathMatcher, name string) bool {
	return matcher.Matches(name)
}
