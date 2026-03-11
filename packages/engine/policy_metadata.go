package engine

import (
	"archive/tar"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/islishude/gotgz/packages/archive"
	"github.com/islishude/gotgz/packages/cli"
)

// resolvePolicy converts CLI options into effective permission policy.
func resolvePolicy(opts cli.Options) PermissionPolicy {
	isRoot := os.Geteuid() == 0
	policy := PermissionPolicy{SameOwner: isRoot, SamePerms: isRoot, NumericOwner: opts.NumericOwner}
	if opts.SameOwner != nil {
		policy.SameOwner = *opts.SameOwner
	}
	if opts.SamePermissions != nil {
		policy.SamePerms = *opts.SamePermissions
	}
	return policy
}

// resolveMetadataPolicy converts CLI options into effective metadata policy.
func resolveMetadataPolicy(opts cli.Options) MetadataPolicy {
	return MetadataPolicy{
		Xattrs: opts.Xattrs,
		ACL:    opts.ACL,
	}
}

// filterACLLikeXattrs removes xattrs that appear to contain ACL payloads.
func filterACLLikeXattrs(attrs map[string][]byte) map[string][]byte {
	if len(attrs) == 0 {
		return attrs
	}
	out := make(map[string][]byte, len(attrs))
	for k, v := range attrs {
		if strings.Contains(strings.ToLower(k), "acl") {
			continue
		}
		out[k] = v
	}
	return out
}

// prepareMetadataForArchive filters metadata before storing it in archive headers.
func prepareMetadataForArchive(xattrs map[string][]byte, acls map[string][]byte, policy MetadataPolicy) (map[string][]byte, map[string][]byte) {
	if !policy.Xattrs {
		xattrs = nil
	} else if !policy.ACL {
		xattrs = filterACLLikeXattrs(xattrs)
	}
	if !policy.ACL {
		acls = nil
	}
	return xattrs, acls
}

// decodeMetadataForExtract decodes archive metadata based on extraction policy.
func decodeMetadataForExtract(hdr *tar.Header, policy MetadataPolicy) (map[string][]byte, map[string][]byte, error) {
	var xattrs map[string][]byte
	var errs []error
	if policy.Xattrs {
		var err error
		xattrs, err = archive.DecodeXattrFromPAX(hdr)
		if err != nil {
			errs = append(errs, fmt.Errorf("decode xattrs: %w", err))
		} else if !policy.ACL {
			xattrs = filterACLLikeXattrs(xattrs)
		}
	}

	var acls map[string][]byte
	if policy.ACL {
		var err error
		acls, err = archive.DecodeACLFromPAX(hdr)
		if err != nil {
			errs = append(errs, fmt.Errorf("decode acls: %w", err))
		}
	}
	return xattrs, acls, errors.Join(errs...)
}
