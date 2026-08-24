package engine

import (
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/islishude/gotgz/packages/archivepath"
)

type localCreateRecordWire struct {
	Current     string
	ArchiveName string
}

// spoolLocalCreateRecords validates and writes one local member plan without
// retaining a record slice proportional to the number of filesystem entries.
func spoolLocalCreateRecords(ctx context.Context, spoolDir, member, chdir string, excludeMatcher *archivepath.CompiledPathMatcher, outputPolicy *createOutputPolicy) (path string, total int64, count int64, retErr error) {
	file, err := os.CreateTemp(spoolDir, "member-*.gob")
	if err != nil {
		return "", 0, 0, fmt.Errorf("create local plan spool: %w", err)
	}
	spoolInfo, err := os.Stat(spoolDir)
	if err != nil {
		_ = file.Close()
		_ = os.Remove(file.Name())
		return "", 0, 0, fmt.Errorf("stat local plan spool directory: %w", err)
	}
	path = file.Name()
	keep := false
	defer func() {
		closeErr := file.Close()
		if retErr == nil {
			retErr = closeErr
		} else {
			retErr = errors.Join(retErr, closeErr)
		}
		if !keep {
			retErr = errors.Join(retErr, removePlanFile(path))
			path = ""
		}
	}()

	encoder := gob.NewEncoder(file)
	retErr = walkLocalCreateMemberEntries(ctx, member, chdir, excludeMatcher, func(record localCreateRecord, entry fs.DirEntry) error {
		if entry.IsDir() {
			info, err := entry.Info()
			if err != nil {
				return err
			}
			if os.SameFile(spoolInfo, info) {
				return filepath.SkipDir
			}
		}
		if outputPolicy.shouldSkipLocal(record.current) {
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 {
			linkTarget, err := os.Readlink(record.current)
			if err != nil {
				return err
			}
			if err := validateCreateSymlinkTarget(record.archiveName, linkTarget); err != nil {
				return err
			}
		}
		if entry.Type().IsRegular() {
			info, err := entry.Info()
			if err != nil {
				return err
			}
			if info.Mode().IsRegular() {
				total = addCreatePlanSize(total, info.Size())
			}
		}
		if err := encoder.Encode(localCreateRecordWire{Current: record.current, ArchiveName: record.archiveName}); err != nil {
			return fmt.Errorf("encode local plan record: %w", err)
		}
		count++
		return nil
	})
	if retErr != nil || count == 0 {
		return path, total, count, retErr
	}
	keep = true
	return path, total, count, nil
}

func removePlanFile(path string) error {
	if path == "" {
		return nil
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove local plan spool %q: %w", path, err)
	}
	return nil
}

// replayLocalCreateRecords decodes one private plan file and refreshes each
// entry's filesystem metadata immediately before the archive writer sees it.
func replayLocalCreateRecords(ctx context.Context, path string, visit func(record localCreateRecord, info fs.FileInfo) error) (retErr error) {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open local plan spool %q: %w", path, err)
	}
	defer func() {
		retErr = errors.Join(retErr, file.Close())
	}()

	decoder := gob.NewDecoder(file)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		var wire localCreateRecordWire
		if err := decoder.Decode(&wire); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("decode local plan spool %q: %w", path, err)
		}
		record := localCreateRecord{current: wire.Current, archiveName: wire.ArchiveName}
		info, err := os.Lstat(record.current)
		if err != nil {
			return err
		}
		if err := visit(record, info); err != nil {
			return err
		}
	}
}
