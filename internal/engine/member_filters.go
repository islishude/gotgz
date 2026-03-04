package engine

import (
	"fmt"
	"os"
	"path"
	"strings"

	"github.com/islishude/gotgz/internal/cli"
)

// shouldSkipMember reports whether a member should be skipped by --wildcards/member filters.
func shouldSkipMember(opts cli.Options, name string) bool {
	if len(opts.Members) == 0 {
		return false
	}
	for _, m := range opts.Members {
		if opts.Wildcards {
			ok, _ := path.Match(m, name)
			if ok {
				return false
			}
			continue
		}
		if m == name {
			return false
		}
	}
	return true
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

// matchExclude reports whether name matches at least one exclude pattern.
func matchExclude(patterns []string, name string) bool {
	for _, p := range patterns {
		if ok, _ := path.Match(p, name); ok {
			return true
		}
	}
	return false
}
