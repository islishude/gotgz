package archive

import (
	"archive/tar"
	"encoding/base64"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	xattrPrefix = "GOTGZ.xattr."
	aclPrefix   = "GOTGZ.acl."
)

func EnsurePAX(hdr *tar.Header) {
	if hdr.PAXRecords == nil {
		hdr.PAXRecords = make(map[string]string)
	}
}

func EncodeXattrToPAX(hdr *tar.Header, attrs map[string][]byte) {
	EnsurePAX(hdr)
	for k, v := range attrs {
		hdr.PAXRecords[xattrPrefix+url.QueryEscape(k)] = base64.StdEncoding.EncodeToString(v)
	}
}

func DecodeXattrFromPAX(hdr *tar.Header) (map[string][]byte, error) {
	out := make(map[string][]byte)
	for k, v := range hdr.PAXRecords {
		if !strings.HasPrefix(k, xattrPrefix) {
			continue
		}
		name, err := url.QueryUnescape(strings.TrimPrefix(k, xattrPrefix))
		if err != nil {
			return nil, fmt.Errorf("decode xattr name: %w", err)
		}
		b, err := base64.StdEncoding.DecodeString(v)
		if err != nil {
			return nil, fmt.Errorf("decode xattr %q: %w", name, err)
		}
		out[name] = b
	}
	return out, nil
}

func EncodeACLToPAX(hdr *tar.Header, acls map[string][]byte) {
	EnsurePAX(hdr)
	for k, v := range acls {
		hdr.PAXRecords[aclPrefix+k] = base64.StdEncoding.EncodeToString(v)
	}
}

func DecodeACLFromPAX(hdr *tar.Header) (map[string][]byte, error) {
	out := make(map[string][]byte)
	for k, v := range hdr.PAXRecords {
		if !strings.HasPrefix(k, aclPrefix) {
			continue
		}
		name := strings.TrimPrefix(k, aclPrefix)
		b, err := base64.StdEncoding.DecodeString(v)
		if err != nil {
			return nil, fmt.Errorf("decode acl %q: %w", name, err)
		}
		out[name] = b
	}
	return out, nil
}

func HeaderToS3Metadata(hdr *tar.Header) (map[string]string, bool) {
	meta := map[string]string{
		"gotgz-type":  strconv.Itoa(int(hdr.Typeflag)),
		"gotgz-mode":  strconv.FormatInt(int64(hdr.Mode), 8),
		"gotgz-uid":   strconv.Itoa(hdr.Uid),
		"gotgz-gid":   strconv.Itoa(hdr.Gid),
		"gotgz-mtime": strconv.FormatInt(hdr.ModTime.Unix(), 10),
	}
	if hdr.Linkname != "" {
		meta["gotgz-linkname"] = hdr.Linkname
	}
	if hdr.Uname != "" {
		meta["gotgz-uname"] = hdr.Uname
	}
	if hdr.Gname != "" {
		meta["gotgz-gname"] = hdr.Gname
	}
	total := 0
	for k, v := range meta {
		total += len(k) + len(v)
	}
	// AWS S3 has a limit of 2KB for user-defined metadata
	// The S3 metadata size limit check is approximate and may not account for AWS metadata encoding overhead.
	return meta, total <= 1500
}

func ParseMTime(v string) time.Time {
	t, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return time.Time{}
	}
	return time.Unix(t, 0)
}
