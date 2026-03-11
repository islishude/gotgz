//go:build unix

package engine

import (
	"io/fs"
	"sync"

	"golang.org/x/sys/unix"
)

var umaskMu sync.Mutex

func currentUmask() fs.FileMode {
	umaskMu.Lock()
	defer umaskMu.Unlock()
	old := unix.Umask(0)
	unix.Umask(old)
	return fs.FileMode(old)
}
