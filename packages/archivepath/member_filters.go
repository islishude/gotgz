package archivepath

import (
	"fmt"
	"os"
	"path"
	"strings"
)

// CompiledPathMatcher matches path-like names against exact strings and globs.
type CompiledPathMatcher struct {
	exactSubtrees    []string
	basenameSubtrees []string
	globs            []string
}

// NewExactPathMatcher stores every value as an exact match.
func NewExactPathMatcher(values []string) *CompiledPathMatcher {
	if len(values) == 0 {
		return nil
	}
	m := &CompiledPathMatcher{exactSubtrees: make([]string, 0, len(values))}
	for _, value := range values {
		m.exactSubtrees = append(m.exactSubtrees, normalizeMatchPath(value))
	}
	return m
}

// NewCompiledPathMatcher classifies patterns so exact matches avoid path.Match.
func NewCompiledPathMatcher(patterns []string) *CompiledPathMatcher {
	if len(patterns) == 0 {
		return nil
	}
	m := &CompiledPathMatcher{
		exactSubtrees:    make([]string, 0),
		basenameSubtrees: make([]string, 0),
		globs:            make([]string, 0),
	}
	for _, pattern := range patterns {
		if strings.ContainsAny(pattern, "*?[") {
			m.globs = append(m.globs, normalizeMatchPattern(pattern))
			continue
		}
		normalized := normalizeMatchPath(pattern)
		if !strings.Contains(normalized, "/") {
			m.basenameSubtrees = append(m.basenameSubtrees, normalized)
		} else {
			m.exactSubtrees = append(m.exactSubtrees, normalized)
		}
	}
	if len(m.exactSubtrees) == 0 && len(m.basenameSubtrees) == 0 && len(m.globs) == 0 {
		return nil
	}
	return m
}

// Matches reports whether name matches at least one stored exact or glob rule.
func (m *CompiledPathMatcher) Matches(name string) bool {
	if m == nil {
		return false
	}
	name = normalizeMatchPath(name)
	if m.matchesStoredSubtree(name) {
		return true
	}
	for _, pattern := range m.globs {
		if matchArchiveGlob(pattern, name) {
			return true
		}
	}
	return false
}

// CoversSubtree reports whether an exact rule matching name also defines all
// descendants of name as matches. Glob rules are intentionally excluded: a
// single-segment * match on a directory does not imply a recursive ** match.
func (m *CompiledPathMatcher) CoversSubtree(name string) bool {
	if m == nil {
		return false
	}
	return m.matchesStoredSubtree(normalizeMatchPath(name))
}

func (m *CompiledPathMatcher) matchesStoredSubtree(name string) bool {
	for _, exact := range m.exactSubtrees {
		if exact == "" || name == exact || strings.HasPrefix(name, exact+"/") {
			return true
		}
	}
	for _, basename := range m.basenameSubtrees {
		for segment := range strings.SplitSeq(name, "/") {
			if segment == basename {
				return true
			}
		}
	}
	return false
}

func normalizeMatchPath(name string) string {
	for strings.HasPrefix(name, "./") {
		name = strings.TrimPrefix(name, "./")
	}
	name = strings.TrimPrefix(name, "/")
	name = path.Clean(name)
	if name == "." {
		return ""
	}
	return strings.TrimSuffix(name, "/")
}

func normalizeMatchPattern(pattern string) string {
	for strings.HasPrefix(pattern, "./") {
		pattern = strings.TrimPrefix(pattern, "./")
	}
	pattern = strings.TrimPrefix(pattern, "/")
	return strings.TrimSuffix(pattern, "/")
}

// matchArchiveGlob matches path segments with ** as the only recursive token.
// A pattern without a slash is matched against every basename in the path.
func matchArchiveGlob(pattern, name string) bool {
	if !strings.Contains(pattern, "/") {
		for segment := range strings.SplitSeq(name, "/") {
			if ok, _ := path.Match(pattern, segment); ok {
				return true
			}
		}
		return false
	}
	return matchArchiveGlobSegments(strings.Split(pattern, "/"), strings.Split(name, "/"))
}

func matchArchiveGlobSegments(pattern, name []string) bool {
	if len(pattern) == 0 {
		return len(name) == 0
	}
	if pattern[0] == "**" {
		for index := 0; index <= len(name); index++ {
			if matchArchiveGlobSegments(pattern[1:], name[index:]) {
				return true
			}
		}
		return false
	}
	if len(name) == 0 {
		return false
	}
	matched, err := path.Match(pattern[0], name[0])
	return err == nil && matched && matchArchiveGlobSegments(pattern[1:], name[1:])
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

// ValidateGlobPatterns validates archive wildcard syntax, including **.
func ValidateGlobPatterns(patterns []string) error {
	for _, pattern := range patterns {
		if err := validateArchiveGlob(pattern); err != nil {
			return fmt.Errorf("invalid member pattern %q: %w", pattern, err)
		}
	}
	return nil
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
		if err := validateArchiveGlob(pattern); err != nil {
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
			if err := validateArchiveGlob(line); err != nil {
				return nil, fmt.Errorf("invalid exclude pattern %q in %s:%d: %w", line, f, lineNo, err)
			}
			out = append(out, line)
		}
	}
	return out, nil
}

func validateArchiveGlob(pattern string) error {
	for segment := range strings.SplitSeq(normalizeMatchPattern(pattern), "/") {
		if segment == "**" {
			continue
		}
		if _, err := path.Match(segment, ""); err != nil {
			return err
		}
	}
	return nil
}

// MatchExcludeWithMatcher reports whether name matches at least one compiled
// exclude pattern.
func MatchExcludeWithMatcher(matcher *CompiledPathMatcher, name string) bool {
	return matcher.Matches(name)
}
