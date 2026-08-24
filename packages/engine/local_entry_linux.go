//go:build linux

package engine

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

func openRegularLocalEntry(record localCreateRecord) (*localEntryHandle, error) {
	fd, err := unix.Open(record.current, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if err != nil {
		return nil, fmt.Errorf("open local archive member %q without following links: %w", record.current, err)
	}
	file := os.NewFile(uintptr(fd), record.current)
	if file == nil {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("open local archive member %q returned an invalid file descriptor", record.current)
	}
	info, err := file.Stat()
	if err != nil {
		return nil, errors.Join(fmt.Errorf("stat opened local archive member %q: %w", record.current, err), file.Close())
	}
	if !info.Mode().IsRegular() {
		return nil, errors.Join(
			fmt.Errorf("local archive member %q changed from a regular file to %s", record.current, info.Mode().Type()),
			file.Close(),
		)
	}
	return newLocalEntryHandle(record, info, file)
}
