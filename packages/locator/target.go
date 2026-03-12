package locator

import (
	"fmt"

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
		r.Path = archivepath.AddSuffix(r.Path, suffix)
		r.Raw = r.Path
	case KindS3:
		r.Key = archivepath.AddSuffix(r.Key, suffix)
	case KindStdio:
		return Ref{}, fmt.Errorf("cannot use -suffix with -f -")
	}
	return r, nil
}
