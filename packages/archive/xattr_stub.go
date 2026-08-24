//go:build !unix

package archive

import "errors"

// ErrXattrsUnsupported indicates that this platform build cannot preserve xattrs.
var ErrXattrsUnsupported = errors.New("extended attributes are not supported on this platform")

func ReadPathMetadata(path string) (map[string][]byte, map[string][]byte, error) {
	return nil, nil, ErrXattrsUnsupported
}

func WritePathMetadata(path string, xattrs map[string][]byte, acls map[string][]byte) error {
	if len(xattrs) == 0 && len(acls) == 0 {
		return nil
	}
	return ErrXattrsUnsupported
}

func XattrsSupported() bool { return false }
func ACLSupported() bool    { return false }
