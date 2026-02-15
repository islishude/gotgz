package cli

import (
	"fmt"
	"strconv"
	"strings"
)

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

type Options struct {
	Mode             Mode
	Archive          string
	Suffix           string
	ACL              bool
	Xattrs           bool
	Verbose          bool
	Help             bool
	CompressionLevel *int
	StripComponents  int
	Chdir            string
	ToStdout         bool
	Compression      CompressionHint
	Exclude          []string
	ExcludeFrom      []string
	Wildcards        bool
	NumericOwner     bool
	SameOwner        *bool
	SamePermissions  *bool
	Members          []string
}

func Parse(args []string) (Options, error) {
	opts := Options{Compression: CompressionAuto}
	if len(args) == 0 {
		return opts, fmt.Errorf("no operation mode specified")
	}

	if legacyToken(args[0]) {
		args = append([]string{"-" + args[0]}, args[1:]...)
	}

	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "--" {
			opts.Members = append(opts.Members, args[i+1:]...)
			break
		}
		if strings.HasPrefix(a, "-compression-level") {
			name, value, hasValue := strings.Cut(strings.TrimPrefix(a, "-"), "=")
			if name != "compression-level" {
				return opts, fmt.Errorf("unsupported option %s", a)
			}
			v, nextI, err := resolveValue(name, value, hasValue, args, i)
			if err != nil {
				return opts, err
			}
			i = nextI
			level, err := strconv.Atoi(v)
			if err != nil || level < 1 || level > 9 {
				return opts, fmt.Errorf("option --compression-level requires an integer between 1 and 9")
			}
			opts.CompressionLevel = &level
			continue
		}
		if strings.HasPrefix(a, "-suffix") {
			name, value, hasValue := strings.Cut(strings.TrimPrefix(a, "-"), "=")
			if name != "suffix" {
				return opts, fmt.Errorf("unsupported option %s", a)
			}
			v, nextI, err := resolveValue(name, value, hasValue, args, i)
			if err != nil {
				return opts, err
			}
			i = nextI
			opts.Suffix = v
			continue
		}
		if !strings.HasPrefix(a, "-") || a == "-" {
			opts.Members = append(opts.Members, args[i:]...)
			break
		}
		if strings.HasPrefix(a, "--") {
			name, value, hasValue := strings.Cut(a[2:], "=")
			switch name {
			case "exclude":
				v, nextI, err := resolveValue(name, value, hasValue, args, i)
				if err != nil {
					return opts, err
				}
				i = nextI
				opts.Exclude = append(opts.Exclude, v)
			case "exclude-from":
				v, nextI, err := resolveValue(name, value, hasValue, args, i)
				if err != nil {
					return opts, err
				}
				i = nextI
				opts.ExcludeFrom = append(opts.ExcludeFrom, v)
			case "strip-components":
				v, nextI, err := resolveValue(name, value, hasValue, args, i)
				if err != nil {
					return opts, err
				}
				i = nextI
				n, err := strconv.Atoi(v)
				if err != nil || n < 0 {
					return opts, fmt.Errorf("option --strip-components requires a non-negative integer")
				}
				opts.StripComponents = n
			case "compression-level":
				v, nextI, err := resolveValue(name, value, hasValue, args, i)
				if err != nil {
					return opts, err
				}
				i = nextI
				level, err := strconv.Atoi(v)
				if err != nil || level < 1 || level > 9 {
					return opts, fmt.Errorf("option --compression-level requires an integer between 1 and 9")
				}
				opts.CompressionLevel = &level
			case "acl":
				opts.ACL = true
			case "xattrs":
				opts.Xattrs = true
			case "suffix":
				v, nextI, err := resolveValue(name, value, hasValue, args, i)
				if err != nil {
					return opts, err
				}
				i = nextI
				opts.Suffix = v
			case "wildcards":
				opts.Wildcards = true
			case "numeric-owner":
				opts.NumericOwner = true
			case "same-owner":
				b := true
				opts.SameOwner = &b
			case "no-same-owner":
				b := false
				opts.SameOwner = &b
			case "same-permissions":
				b := true
				opts.SamePermissions = &b
			case "no-same-permissions":
				b := false
				opts.SamePermissions = &b
			case "zstd":
				opts.Compression = CompressionZstd
			case "lz4":
				opts.Compression = CompressionLz4
			case "help":
				opts.Help = true
			default:
				return opts, fmt.Errorf("unsupported option --%s", name)
			}
			continue
		}

		shorts := a[1:]
		for j := 0; j < len(shorts); j++ {
			s := shorts[j]
			switch s {
			case 'c':
				if err := setMode(&opts, ModeCreate); err != nil {
					return opts, err
				}
			case 'x':
				if err := setMode(&opts, ModeExtract); err != nil {
					return opts, err
				}
			case 't':
				if err := setMode(&opts, ModeList); err != nil {
					return opts, err
				}
			case 'v':
				opts.Verbose = true
			case 'h':
				opts.Help = true
			case 'O':
				opts.ToStdout = true
			case 'z':
				opts.Compression = CompressionGzip
			case 'j':
				opts.Compression = CompressionBzip2
			case 'J':
				opts.Compression = CompressionXz
			case 'f', 'C':
				var val string
				if j+1 < len(shorts) {
					val = shorts[j+1:]
				} else {
					i++
					if i >= len(args) {
						return opts, fmt.Errorf("option -%c requires an argument", s)
					}
					val = args[i]
				}
				if s == 'f' {
					opts.Archive = val
				} else {
					opts.Chdir = val
				}
				j = len(shorts)
			default:
				return opts, fmt.Errorf("unsupported option -%c", s)
			}
		}
	}

	if opts.Help {
		return opts, nil
	}
	if opts.Mode == ModeNone {
		return opts, fmt.Errorf("no operation mode specified")
	}
	if opts.Archive == "" {
		return opts, fmt.Errorf("option -f is required")
	}
	if opts.Mode == ModeCreate && len(opts.Members) == 0 {
		return opts, fmt.Errorf("cowardly refusing to create an empty archive")
	}
	return opts, nil
}

func legacyToken(v string) bool {
	if strings.HasPrefix(v, "-") || v == "" {
		return false
	}
	for _, r := range v {
		switch r {
		case 'c', 'x', 't', 'v', 'f', 'C', 'z', 'j', 'J', 'O':
		default:
			return false
		}
	}
	return true
}

func setMode(opts *Options, mode Mode) error {
	if opts.Mode != ModeNone && opts.Mode != mode {
		return fmt.Errorf("multiple operation modes specified")
	}
	opts.Mode = mode
	return nil
}

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
