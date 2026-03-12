package engine

import (
	"archive/tar"
	"errors"
	"fmt"
	"strings"

	"github.com/islishude/gotgz/packages/archive"
)

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
