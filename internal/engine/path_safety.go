package engine

import (
	"errors"
	"fmt"
	"os"
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
	if err := ensureSymlinkFreePath(base, resolved); err != nil {
		return fmt.Errorf("refusing symlink %q -> %q: %w", symlinkPath, linkname, err)
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

// ensureSymlinkFreeParentPath verifies that candidate's parent chain under
// base does not traverse any pre-existing symbolic links.
func ensureSymlinkFreeParentPath(base, candidate string) error {
	parent := filepath.Dir(filepath.Clean(candidate))
	if parent == filepath.Clean(candidate) {
		return nil
	}
	return ensureSymlinkFreePath(base, parent)
}

// ensureSymlinkFreePath rejects existing symbolic links anywhere in the
// already-present portion of candidate's path below base.
func ensureSymlinkFreePath(base, candidate string) error {
	base = filepath.Clean(base)
	candidate = filepath.Clean(candidate)

	rel, err := filepath.Rel(base, candidate)
	if err != nil {
		return fmt.Errorf("compute relative path for %q: %w", candidate, err)
	}
	if rel == "." {
		return nil
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return fmt.Errorf("refusing to access outside target directory: %s", candidate)
	}

	current := base
	for part := range strings.SplitSeq(rel, string(filepath.Separator)) {
		current = filepath.Join(current, part)
		info, err := os.Lstat(current)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("lstat %q: %w", current, err)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("refusing to follow symlink in extraction path: %s", current)
		}
	}
	return nil
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
