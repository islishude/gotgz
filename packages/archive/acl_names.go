package archive

// IsACLMetadataName classifies the exact ACL xattrs understood by gotgz.
func IsACLMetadataName(name string) bool {
	switch name {
	case "system.posix_acl_access", "system.posix_acl_default", "system.nfs4_acl":
		return true
	default:
		return false
	}
}
