//go:build !unix

package archive

import (
	"io/fs"
	"testing"
)

// TestCurrentUmaskNonUnixFallback verifies the non-unix fallback value.
func TestCurrentUmaskNonUnixFallback(t *testing.T) {
	const want = fs.FileMode(0o022)

	if got := CurrentUmask(); got != want {
		t.Fatalf("CurrentUmask() = %04o, want %04o", got, want)
	}
}
