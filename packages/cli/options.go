package cli

import "os"

type Mode string

const (
	ModeNone    Mode = ""
	ModeCreate  Mode = "c"
	ModeExtract Mode = "x"
	ModeList    Mode = "t"
)

type CompressionHint string

const (
	CompressionAuto  CompressionHint = "auto"
	CompressionNone  CompressionHint = "none"
	CompressionGzip  CompressionHint = "gzip"
	CompressionBzip2 CompressionHint = "bzip2"
	CompressionXz    CompressionHint = "xz"
	CompressionZstd  CompressionHint = "zstd"
	CompressionLz4   CompressionHint = "lz4"
)

type ProgressMode string

const (
	ProgressAuto   ProgressMode = "auto"
	ProgressAlways ProgressMode = "always"
	ProgressNever  ProgressMode = "never"
)

type Options struct {
	Mode             Mode
	Archive          string
	Suffix           string
	SplitSizeBytes   int64
	ACL              bool
	Xattrs           bool
	Verbose          bool
	Help             bool
	Version          bool
	CompressionLevel *int
	StripComponents  int
	Chdir            string
	S3CacheControl   string
	S3ObjectTags     map[string]string
	ToStdout         bool
	Compression      CompressionHint
	Exclude          []string
	ExcludeFrom      []string
	Wildcards        bool
	NumericOwner     bool
	SameOwner        *bool
	SamePermissions  *bool
	Progress         ProgressMode
	Members          []string
}

// PermissionPolicy captures ownership and permission restore behavior for extraction.
type PermissionPolicy struct {
	SameOwner    bool
	SamePerms    bool
	NumericOwner bool
}

// MetadataPolicy controls whether extended attributes and ACL metadata are preserved.
type MetadataPolicy struct {
	Xattrs bool
	ACL    bool
}

// ResolvePermissionPolicy converts parsed options into an effective permission policy.
func (opts Options) ResolvePermissionPolicy() PermissionPolicy {
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

// ResolveMetadataPolicy converts parsed options into an effective metadata policy.
func (opts Options) ResolveMetadataPolicy() MetadataPolicy {
	return MetadataPolicy{
		Xattrs: opts.Xattrs,
		ACL:    opts.ACL,
	}
}
