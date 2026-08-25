package engine

import (
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/islishude/gotgz/packages/archivepath"
)

type createPlanSpoolSink struct {
	encoder *gob.Encoder
}

func (s createPlanSpoolSink) Append(record createPlanRecord) error {
	if err := s.encoder.Encode(record); err != nil {
		return fmt.Errorf("encode local plan record: %w", err)
	}
	return nil
}

// spoolLocalCreateRecords validates and writes one local member plan without
// retaining a record slice proportional to the number of filesystem entries.
func spoolLocalCreateRecords(ctx context.Context, spoolDir, member, chdir string, excludeMatcher *archivepath.CompiledPathMatcher, outputPolicy *createOutputPolicy) (path string, total int64, count int64, retErr error) {
	return spoolLocalCreateRecordsWithLimiter(ctx, spoolDir, member, chdir, excludeMatcher, outputPolicy, nil)
}

func spoolLocalCreateRecordsWithLimiter(ctx context.Context, spoolDir, member, chdir string, excludeMatcher *archivepath.CompiledPathMatcher, outputPolicy *createOutputPolicy, limiter *createPlanMetadataLimiter) (path string, total int64, count int64, retErr error) {
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

	sink := createPlanSpoolSink{encoder: gob.NewEncoder(file)}
	total, count, retErr = scanLocalCreateRecords(
		ctx,
		member,
		chdir,
		excludeMatcher,
		outputPolicy,
		spoolInfo,
		sink,
		newCreatePlanScannerConfig(limiter),
	)
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
func replayLocalCreateRecords(ctx context.Context, path string, visit func(entry *localEntryHandle) error) (retErr error) {
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
		var wire createPlanRecord
		if err := decoder.Decode(&wire); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("decode local plan spool %q: %w", path, err)
		}
		record := localCreateRecord{current: wire.Current, archiveName: wire.ArchiveName}
		entry, err := openLocalEntry(record, wire.EntryType)
		if err != nil {
			return err
		}
		if err := visitLocalEntry(entry, visit); err != nil {
			return err
		}
	}
}
