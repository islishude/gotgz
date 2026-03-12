// Package locator parses archive and member references into normalized
// destination/source descriptors used by engine workflows.
package locator

import (
	"strings"
)

// Kind identifies the backing storage protocol represented by a Ref.
type Kind string

const (
	// KindLocal identifies a local filesystem path.
	KindLocal Kind = "local"
	// KindStdio identifies stdin/stdout stream usage.
	KindStdio Kind = "stdio"
	// KindS3 identifies an S3 object reference.
	KindS3 Kind = "s3"
	// KindHTTP identifies an HTTP(S) archive URL.
	KindHTTP Kind = "http"
)

// Ref is a normalized archive or member reference across supported backends.
type Ref struct {
	// Kind is the parsed backend type.
	Kind Kind
	// Raw is the original reference string from user input.
	Raw string
	// URL stores the HTTP(S) URL when Kind is KindHTTP.
	URL string
	// Path stores the local filesystem path when Kind is KindLocal.
	Path string
	// Bucket stores the S3 bucket name or access-point ARN when Kind is KindS3.
	Bucket string
	// Key stores the S3 object key when Kind is KindS3.
	Key string
	// Metadata stores parsed S3 URI query metadata for uploads.
	Metadata map[string]string
	// ObjectTags stores S3 object tags for upload targets.
	ObjectTags map[string]string
	// CacheControl stores the S3 Cache-Control header for upload targets.
	CacheControl string
}

// ParseArchive parses an archive locator string into a normalized Ref.
func ParseArchive(v string) (Ref, error) {
	if v == "-" {
		return Ref{Kind: KindStdio, Raw: v}, nil
	}
	if strings.HasPrefix(v, "s3://") {
		return parseS3URI(v)
	}
	if strings.HasPrefix(v, "arn:") {
		return parseS3ARN(v)
	}
	if strings.HasPrefix(v, "http://") || strings.HasPrefix(v, "https://") {
		return parseHTTPURI(v)
	}
	return Ref{Kind: KindLocal, Raw: v, Path: v}, nil
}

// ParseMember parses a create/list member input into a normalized Ref.
func ParseMember(v string) (Ref, error) {
	if strings.HasPrefix(v, "s3://") {
		return parseS3URI(v)
	}
	if strings.HasPrefix(v, "arn:") {
		return parseS3ARN(v)
	}
	return Ref{Kind: KindLocal, Raw: v, Path: v}, nil
}
