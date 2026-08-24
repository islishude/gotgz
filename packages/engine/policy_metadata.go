package engine

import (
	"archive/tar"
	"errors"
	"fmt"

	"github.com/islishude/gotgz/packages/archive"
	"github.com/islishude/gotgz/packages/archiveprogress"
	"github.com/islishude/gotgz/packages/cli"
)

// filterACLXattrs removes only the exact ACL xattrs recognized by gotgz.
func filterACLXattrs(attrs map[string][]byte) map[string][]byte {
	if len(attrs) == 0 {
		return attrs
	}
	out := make(map[string][]byte, len(attrs))
	for k, v := range attrs {
		if archive.IsACLMetadataName(k) {
			continue
		}
		out[k] = v
	}
	return out
}

func (r *Runner) effectiveMetadataPolicy(opts cli.Options, reporter *archiveprogress.Reporter) (MetadataPolicy, int) {
	policy := opts.ResolveMetadataPolicy()
	warnings := 0
	if policy.Xattrs && !archive.XattrsSupported() {
		warnings += r.warnf(reporter, "--xattrs is not supported on this platform and will be ignored")
		policy.Xattrs = false
	}
	if policy.ACL && !archive.ACLSupported() {
		warnings += r.warnf(reporter, "--acl is only supported on Linux and will be ignored")
		policy.ACL = false
	}
	return policy, warnings
}

// prepareMetadataForArchive filters metadata before storing it in archive headers.
func prepareMetadataForArchive(xattrs map[string][]byte, acls map[string][]byte, policy MetadataPolicy) (map[string][]byte, map[string][]byte) {
	if !policy.Xattrs {
		xattrs = nil
	} else if !policy.ACL {
		xattrs = filterACLXattrs(xattrs)
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
			xattrs = filterACLXattrs(xattrs)
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
