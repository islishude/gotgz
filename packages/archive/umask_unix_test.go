//go:build unix

package archive

import (
	"io/fs"
	"testing"

	"golang.org/x/sys/unix"
)

// TestCurrentUmaskReturnsCurrentValue verifies that CurrentUmask reports the
// active process umask.
func TestCurrentUmaskReturnsCurrentValue(t *testing.T) {
	const want = fs.FileMode(0o027)

	umaskMu.Lock()
	previous := unix.Umask(int(want))
	umaskMu.Unlock()
	t.Cleanup(func() {
		umaskMu.Lock()
		unix.Umask(previous)
		umaskMu.Unlock()
	})

	if got := CurrentUmask(); got != want {
		t.Fatalf("CurrentUmask() = %04o, want %04o", got, want)
	}
}

// TestCurrentUmaskDoesNotMutate verifies that CurrentUmask restores the umask
// after reading it.
func TestCurrentUmaskDoesNotMutate(t *testing.T) {
	const want = fs.FileMode(0o077)

	umaskMu.Lock()
	previous := unix.Umask(int(want))
	umaskMu.Unlock()
	t.Cleanup(func() {
		umaskMu.Lock()
		unix.Umask(previous)
		umaskMu.Unlock()
	})

	_ = CurrentUmask()
	if got := CurrentUmask(); got != want {
		t.Fatalf("CurrentUmask() after prior call = %04o, want %04o", got, want)
	}
}
