package locator

import (
	"fmt"
	"net/url"

	"maps"
	"strings"

	"github.com/islishude/gotgz/packages/archivepath"
)

// ParseExtractTarget resolves the output target for extract mode and applies S3 options.
func ParseExtractTarget(chdir string, cacheControl string, objectTags map[string]string) (Ref, error) {
	target := chdir
	if target == "" {
		target = "."
	}
	ref, err := ParseArchive(target)
	if err != nil {
		return Ref{}, err
	}
	ref = ref.WithS3CacheControl(cacheControl)
	ref = ref.WithS3ObjectTags(objectTags)
	return ref, nil
}

// WithS3CacheControl sets Cache-Control on S3 refs when the option is provided.
func (r Ref) WithS3CacheControl(cacheControl string) Ref {
	if r.Kind != KindS3 {
		return r
	}
	cacheControl = strings.TrimSpace(cacheControl)
	if cacheControl == "" {
		return r
	}
	r.CacheControl = cacheControl
	return r
}

// WithS3ObjectTags sets S3 object tags on S3 refs when the option is provided.
func (r Ref) WithS3ObjectTags(objectTags map[string]string) Ref {
	if r.Kind != KindS3 {
		return r
	}
	if len(objectTags) == 0 {
		return r
	}
	r.ObjectTags = maps.Clone(objectTags)
	return r
}

// WithArchiveSuffix rewrites archive destinations when create mode uses -suffix.
func (r Ref) WithArchiveSuffix(suffix string) (Ref, error) {
	if suffix == "" {
		return r, nil
	}

	switch r.Kind {
	case KindLocal:
		return r.WithArchiveName(archivepath.AddSuffix(r.Path, suffix))
	case KindS3:
		return r.WithArchiveName(archivepath.AddSuffix(r.Key, suffix))
	case KindStdio:
		return Ref{}, fmt.Errorf("cannot use -suffix with -f -")
	}
	return r, nil
}

// WithArchiveName replaces the local path or S3 key while preserving the
// locator's backend-specific metadata and spelling.
func (r Ref) WithArchiveName(name string) (Ref, error) {
	switch r.Kind {
	case KindLocal:
		r.Path = name
		r.Raw = name
	case KindS3:
		oldKey := r.Key
		r.Key = name
		r.Raw = rewriteS3RawKey(r.Raw, oldKey, r.Key, r.Bucket)
	case KindStdio:
		if name != "-" {
			return Ref{}, fmt.Errorf("cannot rename stdio archive target")
		}
	default:
		return Ref{}, fmt.Errorf("cannot rename archive ref kind %s", r.Kind)
	}
	return r, nil
}

// rewriteS3RawKey preserves the original S3 locator spelling and query while
// replacing only its object key.
func rewriteS3RawKey(raw, oldKey, newKey, bucket string) string {
	if strings.HasPrefix(raw, "s3://") {
		u, err := url.Parse(raw)
		if err == nil {
			u.Path = "/" + newKey
			u.RawPath = ""
			return u.String()
		}
	}
	if strings.HasPrefix(raw, "arn:") && oldKey != "" && strings.HasSuffix(raw, oldKey) {
		return strings.TrimSuffix(raw, oldKey) + newKey
	}
	return fmt.Sprintf("s3://%s/%s", bucket, newKey)
}
