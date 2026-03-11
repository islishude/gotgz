package engine

import (
	"context"
	"io/fs"
	"os"
	"path"
	"path/filepath"
)

// localCreateEntry describes one local filesystem entry normalized for archive creation.
type localCreateEntry struct {
	current     string
	archiveName string
	entry       fs.DirEntry
	info        fs.FileInfo
}

// walkLocalCreateMember normalizes one local create member and visits all non-excluded entries.
func walkLocalCreateMember(ctx context.Context, member string, chdir string, excludeMatcher *compiledPathMatcher, visit func(entry localCreateEntry) error) error {
	basePath := member
	if chdir != "" {
		basePath = filepath.Join(chdir, member)
	}
	cleanMember := path.Clean(filepath.ToSlash(member))

	return filepath.WalkDir(basePath, func(current string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		rel, err := filepath.Rel(basePath, current)
		if err != nil {
			return err
		}
		archiveName := cleanMember
		if rel != "." {
			archiveName = path.Join(cleanMember, filepath.ToSlash(rel))
		}
		if matchExcludeWithMatcher(excludeMatcher, archiveName) {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}

		info, err := os.Lstat(current)
		if err != nil {
			return err
		}
		return visit(localCreateEntry{
			current:     current,
			archiveName: archiveName,
			entry:       d,
			info:        info,
		})
	})
}
