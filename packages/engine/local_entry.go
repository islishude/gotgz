package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"

	"github.com/islishude/gotgz/packages/archiveprogress"
	"github.com/islishude/gotgz/packages/archiveutil"
)

// localEntryHandle binds refreshed metadata to an optional already-open
// regular file. Linux uses the same fd for fstat and payload reads.
type localEntryHandle struct {
	record     localCreateRecord
	info       fs.FileInfo
	file       *os.File
	linkTarget string
}

func openLocalEntry(record localCreateRecord, scannedType fs.FileMode) (*localEntryHandle, error) {
	if scannedType.IsRegular() {
		return openRegularLocalEntry(record)
	}
	info, err := os.Lstat(record.current)
	if err != nil {
		return nil, err
	}
	return newLocalEntryHandle(record, info, nil)
}

func newLocalEntryHandle(record localCreateRecord, info fs.FileInfo, file *os.File) (*localEntryHandle, error) {
	if info == nil {
		return nil, errors.Join(fmt.Errorf("local archive member %q has no metadata", record.current), closeLocalEntryFile(file))
	}
	entry := &localEntryHandle{record: record, info: info, file: file}
	if info.Mode()&os.ModeSymlink == 0 {
		return entry, nil
	}
	linkTarget, err := os.Readlink(record.current)
	if err != nil {
		return nil, errors.Join(err, entry.Close())
	}
	if err := validateCreateSymlinkTarget(record.archiveName, linkTarget); err != nil {
		return nil, errors.Join(err, entry.Close())
	}
	entry.linkTarget = linkTarget
	return entry, nil
}

func (h *localEntryHandle) payloadReader() (io.Reader, error) {
	if h == nil || h.info == nil || !h.info.Mode().IsRegular() {
		return nil, fmt.Errorf("local archive member is not a regular file")
	}
	if h.file == nil {
		file, err := os.Open(h.record.current)
		if err != nil {
			return nil, err
		}
		h.file = file
	}
	return h.file, nil
}

func copyLocalEntryPayload(ctx context.Context, dst io.Writer, entry *localEntryHandle, reporter *archiveprogress.Reporter) error {
	reader, err := entry.payloadReader()
	if err != nil {
		return err
	}
	size := entry.info.Size()
	if size < 0 {
		return fmt.Errorf("local archive member %q has negative size %d", entry.record.current, size)
	}
	limited := io.LimitReader(reader, size)
	written, err := archiveutil.CopyWithContext(ctx, dst, archiveprogress.NewCountingReader(limited, reporter))
	if err != nil {
		return err
	}
	if written != size {
		return fmt.Errorf("read local archive member %q: copied %d of %d bytes: %w", entry.record.current, written, size, io.ErrUnexpectedEOF)
	}
	return nil
}

func (h *localEntryHandle) Close() error {
	if h == nil || h.file == nil {
		return nil
	}
	file := h.file
	h.file = nil
	return file.Close()
}

func closeLocalEntryFile(file *os.File) error {
	if file == nil {
		return nil
	}
	return file.Close()
}

func visitLocalEntry(entry *localEntryHandle, visit func(*localEntryHandle) error) (retErr error) {
	defer func() {
		retErr = errors.Join(retErr, entry.Close())
	}()
	return visit(entry)
}
