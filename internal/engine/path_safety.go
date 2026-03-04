package engine

import (
	"fmt"
	"path"
	"path/filepath"
	"strings"
)

// safeSymlinkTarget validates that a symlink's target does not escape the
// extraction base directory. linkname is the raw target from the archive;
// symlinkPath is the absolute path where the symlink will be created.
// Absolute symlink targets are always rejected because os.Symlink would
// create a link pointing outside the extraction directory.
func safeSymlinkTarget(base, symlinkPath, linkname string) error {
	if linkname == "" {
		return fmt.Errorf("symlink target is empty")
	}
	if filepath.IsAbs(linkname) {
		return fmt.Errorf("refusing symlink %q -> %q: absolute symlink target not allowed", symlinkPath, linkname)
	}
	base = filepath.Clean(base)

	// Relative symlink targets are resolved from the symlink's parent.
	resolved := filepath.Clean(filepath.Join(filepath.Dir(symlinkPath), filepath.FromSlash(linkname)))

	rel, err := filepath.Rel(base, resolved)
	if err != nil {
		return fmt.Errorf("refusing symlink: cannot compute relative path: %w", err)
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return fmt.Errorf("refusing symlink %q -> %q: target escapes extraction directory", symlinkPath, linkname)
	}
	return nil
}

// safeJoin joins a member path under base and rejects path traversal escapes.
func safeJoin(base, member string) (string, error) {
	base = filepath.Clean(base)
	member = strings.TrimPrefix(member, "/")
	candidate := filepath.Join(base, filepath.FromSlash(member))
	candidate = filepath.Clean(candidate)
	rel, err := filepath.Rel(base, candidate)
	if err != nil {
		return "", err
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("refusing to write outside target directory: %s", member)
	}
	return candidate, nil
}

// stripPathComponents drops leading path components from a member name.
func stripPathComponents(name string, count int) (string, bool) {
	if count <= 0 {
		return name, true
	}
	clean := path.Clean(strings.TrimPrefix(name, "/"))
	parts := make([]string, 0)
	for p := range strings.SplitSeq(clean, "/") {
		if p == "" || p == "." {
			continue
		}
		parts = append(parts, p)
	}
	if len(parts) <= count {
		return "", false
	}
	return strings.Join(parts[count:], "/"), true
}
