//go:build unix && !linux

package archive

// ACLSupported reports that native ACL preservation is unavailable in the
// CGO-free non-Linux builds shipped by gotgz.
func ACLSupported() bool { return false }
