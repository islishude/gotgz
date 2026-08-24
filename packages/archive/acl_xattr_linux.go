//go:build linux

package archive

// ACLSupported reports whether this build preserves Linux ACL xattrs.
func ACLSupported() bool { return true }
