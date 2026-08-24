package engine

import (
	"fmt"
	"path"
	"path/filepath"
	"strings"
)

// validateCreateSymlinkTarget ensures a symlink archived by gotgz can be
// recreated by the default safe extractor without escaping its target root.
func validateCreateSymlinkTarget(archiveName, linkTarget string) error {
	if linkTarget == "" {
		return fmt.Errorf("symlink %q has an empty target", archiveName)
	}
	if filepath.IsAbs(linkTarget) || path.IsAbs(filepath.ToSlash(linkTarget)) {
		return fmt.Errorf("refusing to archive symlink %q -> %q: absolute target not allowed", archiveName, linkTarget)
	}

	name := strings.TrimPrefix(path.Clean(filepath.ToSlash(archiveName)), "/")
	resolved := path.Clean(path.Join(path.Dir(name), filepath.ToSlash(linkTarget)))
	if resolved == ".." || strings.HasPrefix(resolved, "../") {
		return fmt.Errorf("refusing to archive symlink %q -> %q: target escapes archive root", archiveName, linkTarget)
	}
	return nil
}
