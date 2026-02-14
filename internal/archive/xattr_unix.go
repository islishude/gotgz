//go:build unix

package archive

import (
	"bytes"
	"strings"

	"golang.org/x/sys/unix"
)

func ReadPathMetadata(path string) (map[string][]byte, map[string][]byte, error) {
	names, err := listXattr(path)
	if err != nil {
		return nil, nil, err
	}
	xattrs := make(map[string][]byte)
	acls := make(map[string][]byte)
	for _, name := range names {
		v, err := getXattr(path, name)
		if err != nil {
			continue
		}
		xattrs[name] = v
		if strings.Contains(name, "acl") {
			acls[name] = v
		}
	}
	return xattrs, acls, nil
}

func WritePathMetadata(path string, xattrs map[string][]byte, acls map[string][]byte) error {
	for k, v := range xattrs {
		if err := unix.Setxattr(path, k, v, 0); err != nil {
			continue
		}
	}
	for k, v := range acls {
		if err := unix.Setxattr(path, k, v, 0); err != nil {
			continue
		}
	}
	return nil
}

func listXattr(path string) ([]string, error) {
	sz, err := unix.Listxattr(path, nil)
	if err != nil || sz <= 0 {
		return nil, err
	}
	buf := make([]byte, sz)
	n, err := unix.Listxattr(path, buf)
	if err != nil {
		return nil, err
	}
	raw := bytes.Split(buf[:n], []byte{0})
	out := make([]string, 0, len(raw))
	for _, r := range raw {
		if len(r) == 0 {
			continue
		}
		out = append(out, string(r))
	}
	return out, nil
}

func getXattr(path string, key string) ([]byte, error) {
	sz, err := unix.Getxattr(path, key, nil)
	if err != nil || sz <= 0 {
		return nil, err
	}
	buf := make([]byte, sz)
	_, err = unix.Getxattr(path, key, buf)
	if err != nil {
		return nil, err
	}
	return buf, nil
}
