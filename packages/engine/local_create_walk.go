package engine

import (
	"context"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/islishude/gotgz/packages/archivepath"
)

// localCreateRecord identifies one local filesystem entry normalized for archive creation.
type localCreateRecord struct {
	current     string
	archiveName string
}

// walkLocalCreateMember normalizes one local create member and visits all non-excluded entries.
func walkLocalCreateMember(ctx context.Context, member string, chdir string, excludeMatcher *archivepath.CompiledPathMatcher, visit func(record localCreateRecord, info fs.FileInfo) error) error {
	basePath := member
	if chdir != "" {
		basePath = filepath.Join(chdir, member)
	}
	basePath = filepath.Clean(basePath)
	cleanMember := path.Clean(filepath.ToSlash(member))
	basePrefix := localCreateBasePrefix(basePath)

	return filepath.WalkDir(basePath, func(current string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		archiveName, err := localCreateArchiveName(basePath, basePrefix, current, cleanMember)
		if err != nil {
			return err
		}
		if archivepath.MatchExcludeWithMatcher(excludeMatcher, archiveName) {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}

		info, err := os.Lstat(current)
		if err != nil {
			return err
		}
		return visit(localCreateRecord{
			current:     current,
			archiveName: archiveName,
		}, info)
	})
}

// collectLocalCreateRecords walks one local member once, returning normalized
// archive records together with the total regular-file size seen during the scan.
func collectLocalCreateRecords(ctx context.Context, member string, chdir string, excludeMatcher *archivepath.CompiledPathMatcher) ([]localCreateRecord, int64, error) {
	records := make([]localCreateRecord, 0)
	var total int64
	err := walkLocalCreateMember(ctx, member, chdir, excludeMatcher, func(record localCreateRecord, info fs.FileInfo) error {
		records = append(records, record)
		if info.Mode().IsRegular() {
			total += info.Size()
		}
		return nil
	})
	return records, total, err
}

// localCreateBasePrefix returns the fast-path prefix used to derive archive
// member names without calling filepath.Rel for every visited path.
func localCreateBasePrefix(basePath string) string {
	if basePath == "." {
		return ""
	}
	if filepath.Dir(basePath) == basePath {
		return basePath
	}
	return basePath + string(filepath.Separator)
}

// localCreateArchiveName derives one archive member name from the current walk
// path, falling back to filepath.Rel only for unexpected path layouts.
func localCreateArchiveName(basePath, basePrefix, current, cleanMember string) (string, error) {
	if current == basePath {
		return cleanMember, nil
	}
	if basePrefix == "" {
		return joinLocalCreateArchiveName(cleanMember, current), nil
	}
	if strings.HasPrefix(current, basePrefix) {
		return joinLocalCreateArchiveName(cleanMember, current[len(basePrefix):]), nil
	}

	rel, err := filepath.Rel(basePath, current)
	if err != nil {
		return "", err
	}
	if rel == "." {
		return cleanMember, nil
	}
	return joinLocalCreateArchiveName(cleanMember, rel), nil
}

// joinLocalCreateArchiveName appends one relative walk suffix onto the cleaned
// member root while preserving the existing "." semantics.
func joinLocalCreateArchiveName(cleanMember, rel string) string {
	rel = filepath.ToSlash(rel)
	switch cleanMember {
	case ".":
		return rel
	case "/":
		return cleanMember + strings.TrimPrefix(rel, "/")
	default:
		return cleanMember + "/" + rel
	}
}
