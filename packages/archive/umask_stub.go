//go:build !unix

package archive

import "io/fs"

// CurrentUmask returns a conventional default umask on non-unix platforms.
func CurrentUmask() fs.FileMode {
	return 0o022
}
