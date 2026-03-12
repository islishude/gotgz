//go:build unix

package archive

import (
	"io/fs"
	"sync"

	"golang.org/x/sys/unix"
)

var umaskMu sync.Mutex

// CurrentUmask returns the process umask without changing its effective value.
func CurrentUmask() fs.FileMode {
	umaskMu.Lock()
	defer umaskMu.Unlock()
	old := unix.Umask(0)
	unix.Umask(old)
	return fs.FileMode(old)
}
