package archiveutil

import "strings"

// Snapshot identifies one immutable view of a remote archive object.
type Snapshot struct {
	Size         int64
	ETag         string
	VersionID    string
	LastModified string
	URL          string
}

// SupportsRangeFencing reports whether range requests can be bound to this
// snapshot for the given backend.
func (s Snapshot) SupportsRangeFencing(s3 bool) bool {
	if s3 {
		return strings.TrimSpace(s.VersionID) != "" || strings.TrimSpace(s.ETag) != ""
	}
	etag := strings.TrimSpace(s.ETag)
	return (etag != "" && !strings.HasPrefix(etag, "W/")) || strings.TrimSpace(s.LastModified) != ""
}
