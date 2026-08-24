package archive

import "testing"

func TestIsACLMetadataNameUsesExactKnownNames(t *testing.T) {
	for _, name := range []string{"system.posix_acl_access", "system.posix_acl_default", "system.nfs4_acl"} {
		if !IsACLMetadataName(name) {
			t.Fatalf("IsACLMetadataName(%q) = false", name)
		}
	}
	for _, name := range []string{"user.gotgz.acl", "user.acl-cache", "system.posix_acl_access.extra"} {
		if IsACLMetadataName(name) {
			t.Fatalf("IsACLMetadataName(%q) = true for ordinary xattr", name)
		}
	}
}
