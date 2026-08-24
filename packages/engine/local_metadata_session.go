package engine

import (
	"io/fs"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/islishude/gotgz/packages/archive"
	"github.com/islishude/gotgz/packages/archiveprogress"
)

type localMetadataRecord struct {
	target       string
	archiveName  string
	perm         fs.FileMode
	modTime      time.Time
	uid          int
	gid          int
	restoreOwner bool
	isSymlink    bool
	xattrs       map[string][]byte
	acls         map[string][]byte
}

// localMetadataSession applies file metadata immediately and defers directory
// metadata until all descendants have been written.
type localMetadataSession struct {
	runner   *Runner
	reporter *archiveprogress.Reporter
	dirs     map[string]localMetadataRecord
}

func newLocalMetadataSession(runner *Runner, reporter *archiveprogress.Reporter) *localMetadataSession {
	return &localMetadataSession{
		runner:   runner,
		reporter: reporter,
		dirs:     make(map[string]localMetadataRecord),
	}
}

func (s *localMetadataSession) queueDirectory(record localMetadataRecord) {
	if s == nil {
		return
	}
	s.dirs[record.target] = record
}

func (s *localMetadataSession) apply(record localMetadataRecord) int {
	if s == nil {
		return 0
	}
	warnings := 0
	if record.restoreOwner {
		if err := os.Lchown(record.target, record.uid, record.gid); err != nil {
			warnings += s.runner.warnf(s.reporter, "extract: owner for %s could not be restored: %v", record.archiveName, err)
		}
	}
	if !record.isSymlink {
		if err := os.Chmod(record.target, record.perm); err != nil {
			warnings += s.runner.warnf(s.reporter, "extract: permissions for %s could not be restored: %v", record.archiveName, err)
		}
	}
	if err := archive.WritePathMetadata(record.target, record.xattrs, record.acls); err != nil {
		warnings += s.runner.warnf(s.reporter, "extract: metadata for %s could not be fully restored: %v", record.archiveName, err)
	}
	if !record.modTime.IsZero() && !record.isSymlink {
		if err := os.Chtimes(record.target, record.modTime, record.modTime); err != nil {
			warnings += s.runner.warnf(s.reporter, "extract: timestamp for %s could not be restored: %v", record.archiveName, err)
		}
	}
	return warnings
}

func (s *localMetadataSession) finish() int {
	if s == nil || len(s.dirs) == 0 {
		return 0
	}
	records := make([]localMetadataRecord, 0, len(s.dirs))
	for _, record := range s.dirs {
		records = append(records, record)
	}
	sort.SliceStable(records, func(i, j int) bool {
		return strings.Count(records[i].target, string(os.PathSeparator)) > strings.Count(records[j].target, string(os.PathSeparator))
	})
	warnings := 0
	for _, record := range records {
		warnings += s.apply(record)
	}
	return warnings
}
