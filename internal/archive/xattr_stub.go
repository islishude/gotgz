//go:build !unix

package archive

func ReadPathMetadata(path string) (map[string][]byte, map[string][]byte, error) {
	return nil, nil, nil
}

func WritePathMetadata(path string, xattrs map[string][]byte, acls map[string][]byte) error {
	return nil
}
