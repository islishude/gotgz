package cli

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/islishude/gotgz/packages/archivepath"
	"github.com/islishude/gotgz/packages/archiveutil"
	"github.com/islishude/gotgz/packages/compress"
	"github.com/islishude/gotgz/packages/locator"
)

var reservedSplitSuffixPattern = regexp.MustCompile(`(?i)^part[0-9]+$`)

// finalizeOptions applies post-parse validation and required-field checks.
func finalizeOptions(opts Options) (Options, error) {
	if opts.Help || opts.Version {
		return opts, nil
	}
	validated, err := validateOptions(opts)
	if err != nil {
		return opts, err
	}
	if validated.Mode == ModeNone {
		return validated, fmt.Errorf("no operation mode specified")
	}
	if validated.Archive == "" {
		return validated, fmt.Errorf("option -f is required")
	}
	if validated.Mode == ModeCreate && len(validated.Members) == 0 {
		return validated, fmt.Errorf("cowardly refusing to create an empty archive")
	}
	return validated, nil
}

// validateOptions performs cross-field validation after flag parsing.
func validateOptions(opts Options) (Options, error) {
	if opts.Wildcards {
		if err := archivepath.ValidateGlobPatterns(opts.Members); err != nil {
			return opts, err
		}
	}
	if opts.Suffix != "" {
		if opts.Mode != ModeCreate {
			return opts, fmt.Errorf("option --suffix is only supported in create mode")
		}
		if err := validateSuffix(opts.Suffix); err != nil {
			return opts, err
		}
	}
	if opts.Mode == ModeNone || opts.Archive == "" {
		return opts, nil
	}

	ref, err := locator.ParseArchive(opts.Archive)
	if err != nil {
		return opts, err
	}
	if opts.Mode == ModeCreate {
		opts, ref, err = resolveCreateArchiveOutputAt(opts, ref, time.Now())
		if err != nil {
			return opts, err
		}
	}
	return opts, validateResolvedSplit(opts, ref)
}

// ResolveCreateArchiveOutput resolves a programmatically constructed create
// option set before engine opens its destination.
func ResolveCreateArchiveOutput(opts Options) (Options, error) {
	if opts.Mode != ModeCreate {
		return opts, nil
	}
	if opts.Compression == "" {
		opts.Compression = CompressionAuto
	}
	if opts.ResolvedArchive != "" {
		ref, err := locator.ParseArchive(opts.ResolvedArchive)
		if err != nil {
			return opts, err
		}
		opts, err = normalizeCreateArchiveOutput(opts, ref)
		if err != nil {
			return opts, err
		}
		return opts, validateResolvedSplit(opts, ref)
	}
	ref, err := locator.ParseArchive(opts.Archive)
	if err != nil {
		return opts, err
	}
	resolved, resolvedRef, err := resolveCreateArchiveOutputAt(opts, ref, time.Now())
	if err != nil {
		return resolved, err
	}
	return resolved, validateResolvedSplit(resolved, resolvedRef)
}

func validateResolvedSplit(opts Options, ref locator.Ref) error {
	if opts.SplitSizeBytes <= 0 {
		return nil
	}
	if opts.Mode != ModeCreate {
		return fmt.Errorf("option --split-size is only supported in create mode")
	}
	if ref.Kind == locator.KindStdio {
		return fmt.Errorf("option --split-size does not support -f -")
	}
	if opts.Compression == CompressionXz {
		return fmt.Errorf("option --split-size does not support %s compression", opts.Compression)
	}
	if _, ok := archivepath.ParseSplit(archiveutil.NameHint(ref)); ok {
		return fmt.Errorf("option --split-size cannot use an archive name that already contains .partNNNN")
	}
	return nil
}

// resolveCreateArchiveOutputAt applies suffix exactly once and normalizes the
// output format using the final destination name.
func resolveCreateArchiveOutputAt(opts Options, ref locator.Ref, now time.Time) (Options, locator.Ref, error) {
	if ref.Kind == locator.KindHTTP {
		return opts, ref, fmt.Errorf("unsupported archive target %q: http(s) archives are source-only", ref.Raw)
	}
	if opts.Compression == "" {
		opts.Compression = CompressionAuto
	}
	if opts.Suffix != "" {
		if err := validateSuffix(opts.Suffix); err != nil {
			return opts, ref, err
		}
		resolvedName := archivepath.AddSuffixAt(archiveutil.NameHint(ref), opts.Suffix, now)
		var err error
		ref, err = ref.WithArchiveName(resolvedName)
		if err != nil {
			return opts, ref, err
		}
	}
	opts.ResolvedArchive = ref.Raw
	var err error
	opts, err = normalizeCreateArchiveOutput(opts, ref)
	return opts, ref, err
}

func validateSuffix(suffix string) error {
	if suffix == "." || suffix == ".." || strings.ContainsAny(suffix, "/\\\x00") {
		return fmt.Errorf("option --suffix must be a filename suffix without path separators")
	}
	if reservedSplitSuffixPattern.MatchString(suffix) {
		return fmt.Errorf("option --suffix cannot use reserved split name %q", suffix)
	}
	return nil
}

// normalizeCreateArchiveOutput resolves create-mode archive selection from the archive name.
//
// `.zip` selects zip output, while tar-family suffixes resolve the final
// compressor for tar output. Unknown suffixes fall back to uncompressed tar.
func normalizeCreateArchiveOutput(opts Options, ref locator.Ref) (Options, error) {
	if opts.Mode != ModeCreate {
		return opts, nil
	}
	if ref.Kind == locator.KindStdio {
		if opts.Compression == CompressionAuto {
			opts.Compression = CompressionNone
		}
		return opts, nil
	}

	archiveName := archiveutil.NameHint(ref)
	if archiveutil.HasZipHint(archiveName) {
		if isExplicitCompression(opts.Compression) {
			return opts, compressionMismatchError(opts.Compression, archiveName, "zip archive format")
		}
		return opts, nil
	}

	implied := compressionHintFromType(compress.DetectTypeByPath(archiveName))
	if opts.Compression == CompressionAuto {
		if implied == CompressionAuto {
			opts.Compression = CompressionNone
		} else {
			opts.Compression = implied
		}
		return opts, nil
	}
	if opts.Compression != implied {
		return opts, compressionMismatchError(opts.Compression, archiveName, describeCompressionHint(implied))
	}
	return opts, nil
}

// compressionHintFromType maps shared compression types back into CLI hints.
func compressionHintFromType(t compress.Type) CompressionHint {
	switch t {
	case compress.None:
		return CompressionNone
	case compress.Gzip:
		return CompressionGzip
	case compress.Bzip2:
		return CompressionBzip2
	case compress.Xz:
		return CompressionXz
	case compress.Zstd:
		return CompressionZstd
	case compress.Lz4:
		return CompressionLz4
	default:
		return CompressionAuto
	}
}

// isExplicitCompression reports whether the user requested a tar-family compressor.
func isExplicitCompression(v CompressionHint) bool {
	return v != CompressionAuto && v != CompressionNone
}

// describeCompressionHint formats the archive name implication for user-facing errors.
func describeCompressionHint(v CompressionHint) string {
	switch v {
	case CompressionNone:
		return "no compression"
	case CompressionAuto:
		return "no recognized compression suffix"
	default:
		return fmt.Sprintf("%q compression", v)
	}
}

// compressionMismatchError explains why an explicit compressor conflicts with the archive name.
func compressionMismatchError(explicit CompressionHint, archiveName string, implied string) error {
	return fmt.Errorf("compression %q does not match archive name %q (implies %s)", explicit, archiveName, implied)
}
