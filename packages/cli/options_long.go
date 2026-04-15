package cli

import (
	"fmt"
	"strconv"
	"strings"
)

// applyLongOption updates parser state for one normalized long option name.
func (p *optionParser) applyLongOption(name, value string, hasValue bool, i int) (int, error) {
	switch name {
	case "create":
		return i, setMode(&p.opts, ModeCreate)
	case "extract":
		return i, setMode(&p.opts, ModeExtract)
	case "list":
		return i, setMode(&p.opts, ModeList)
	case "exclude":
		v, nextI, err := resolveValue(name, value, hasValue, p.args, i)
		if err != nil {
			return i, err
		}
		p.opts.Exclude = append(p.opts.Exclude, v)
		return nextI, nil
	case "exclude-from":
		v, nextI, err := resolveValue(name, value, hasValue, p.args, i)
		if err != nil {
			return i, err
		}
		p.opts.ExcludeFrom = append(p.opts.ExcludeFrom, v)
		return nextI, nil
	case "strip-components":
		v, nextI, err := resolveValue(name, value, hasValue, p.args, i)
		if err != nil {
			return i, err
		}
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			return i, fmt.Errorf("option --strip-components requires a non-negative integer")
		}
		p.opts.StripComponents = n
		return nextI, nil
	case "compression-level":
		level, nextI, err := p.parseCompressionLevel(name, value, hasValue, i)
		if err != nil {
			return i, err
		}
		p.opts.CompressionLevel = &level
		return nextI, nil
	case "split-size":
		size, nextI, err := p.parseSplitSizeOption(name, value, hasValue, i)
		if err != nil {
			return i, err
		}
		p.opts.SplitSizeBytes = size
		return nextI, nil
	case "acl":
		p.opts.ACL = true
	case "xattrs":
		p.opts.Xattrs = true
	case "suffix":
		v, nextI, err := resolveValue(name, value, hasValue, p.args, i)
		if err != nil {
			return i, err
		}
		p.opts.Suffix = v
		return nextI, nil
	case "cd", "directory":
		v, nextI, err := resolveValue(name, value, hasValue, p.args, i)
		if err != nil {
			return i, err
		}
		p.opts.Chdir = v
		return nextI, nil
	case "s3-cache-control":
		v, nextI, err := resolveValue(name, value, hasValue, p.args, i)
		if err != nil {
			return i, err
		}
		p.opts.S3CacheControl = strings.TrimSpace(v)
		return nextI, nil
	case "s3-tag":
		v, nextI, err := resolveValue(name, value, hasValue, p.args, i)
		if err != nil {
			return i, err
		}
		key, tagValue, err := parseS3Tag(v)
		if err != nil {
			return i, err
		}
		if p.opts.S3ObjectTags == nil {
			p.opts.S3ObjectTags = make(map[string]string)
		}
		p.opts.S3ObjectTags[key] = tagValue
		return nextI, nil
	case "wildcards":
		p.opts.Wildcards = true
	case "numeric-owner":
		p.opts.NumericOwner = true
	case "same-owner":
		p.setSameOwner(true)
	case "no-same-owner":
		p.setSameOwner(false)
	case "same-permissions":
		p.setSamePermissions(true)
	case "no-same-permissions":
		p.setSamePermissions(false)
	case "zstd":
		p.opts.Compression = CompressionZstd
	case "lz4":
		p.opts.Compression = CompressionLz4
	case "gzip", "gunzip":
		p.opts.Compression = CompressionGzip
	case "bzip", "bzip2":
		p.opts.Compression = CompressionBzip2
	case "xz":
		p.opts.Compression = CompressionXz
	case "to-stdout":
		p.opts.ToStdout = true
	case "help":
		p.opts.Help = true
	case "version":
		p.opts.Version = true
	case "progress":
		p.opts.Progress = ProgressAlways
	case "no-progress":
		p.opts.Progress = ProgressNever
	default:
		return i, fmt.Errorf("unsupported option --%s", name)
	}
	return i, nil
}

// parseCompressionLevel validates one compression-level option value.
func (p *optionParser) parseCompressionLevel(name, value string, hasValue bool, i int) (int, int, error) {
	v, nextI, err := resolveValue(name, value, hasValue, p.args, i)
	if err != nil {
		return 0, i, err
	}
	level, err := strconv.Atoi(v)
	if err != nil || level < 1 || level > 9 {
		return 0, i, fmt.Errorf("option --compression-level requires an integer between 1 and 9")
	}
	return level, nextI, nil
}

// parseS3Tag validates one --s3-tag key=value option.
func parseS3Tag(v string) (string, string, error) {
	key, value, ok := strings.Cut(v, "=")
	if !ok {
		return "", "", fmt.Errorf("option --s3-tag requires key=value")
	}
	key = strings.TrimSpace(key)
	if key == "" {
		return "", "", fmt.Errorf("option --s3-tag requires a non-empty key")
	}
	return key, strings.TrimSpace(value), nil
}

// parseSplitSizeOption validates one split-size option value.
func (p *optionParser) parseSplitSizeOption(name, value string, hasValue bool, i int) (int64, int, error) {
	v, nextI, err := resolveValue(name, value, hasValue, p.args, i)
	if err != nil {
		return 0, i, err
	}
	size, err := parseSplitSize(v)
	if err != nil {
		return 0, i, err
	}
	return size, nextI, nil
}

// setSameOwner stores an explicit same-owner choice.
func (p *optionParser) setSameOwner(v bool) {
	p.opts.SameOwner = &v
}

// setSamePermissions stores an explicit same-permissions choice.
func (p *optionParser) setSamePermissions(v bool) {
	p.opts.SamePermissions = &v
}

// resolveValue resolves a long-option value from either an inline suffix or the following argv token.
func resolveValue(name, inline string, hasInline bool, args []string, i int) (string, int, error) {
	if hasInline {
		return inline, i, nil
	}
	i++
	if i >= len(args) {
		return "", i, fmt.Errorf("option --%s requires a value", name)
	}
	return args[i], i, nil
}
