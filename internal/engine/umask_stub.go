//go:build !unix

package engine

import "io/fs"

func currentUmask() fs.FileMode {
	return 0o022
}
