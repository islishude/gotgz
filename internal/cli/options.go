package cli

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"

	"github.com/islishude/gotgz/internal/archivepath"
	"github.com/islishude/gotgz/internal/archiveutil"
	"github.com/islishude/gotgz/internal/locator"
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
	CompressionLevel *int
	StripComponents  int
	Chdir            string
	S3CacheControl   string
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

func Parse(args []string) (Options, error) {
	opts := Options{Compression: CompressionAuto, Progress: ProgressAuto}
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
		if strings.HasPrefix(a, "-split-size") {
			name, value, hasValue := strings.Cut(strings.TrimPrefix(a, "-"), "=")
			if name != "split-size" {
				return opts, fmt.Errorf("unsupported option %s", a)
			}
			v, nextI, err := resolveValue(name, value, hasValue, args, i)
			if err != nil {
				return opts, err
			}
			i = nextI
			size, err := parseSplitSize(v)
			if err != nil {
				return opts, err
			}
			opts.SplitSizeBytes = size
			continue
		}
		if !strings.HasPrefix(a, "-") || a == "-" {
			opts.Members = append(opts.Members, args[i:]...)
			break
		}
		if strings.HasPrefix(a, "--") {
			name, value, hasValue := strings.Cut(a[2:], "=")
			switch name {
			case "create":
				if err := setMode(&opts, ModeCreate); err != nil {
					return opts, err
				}
			case "extract":
				if err := setMode(&opts, ModeExtract); err != nil {
					return opts, err
				}
			case "list":
				if err := setMode(&opts, ModeList); err != nil {
					return opts, err
				}
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
			case "split-size":
				v, nextI, err := resolveValue(name, value, hasValue, args, i)
				if err != nil {
					return opts, err
				}
				i = nextI
				size, err := parseSplitSize(v)
				if err != nil {
					return opts, err
				}
				opts.SplitSizeBytes = size
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
			case "cd", "directory":
				v, nextI, err := resolveValue(name, value, hasValue, args, i)
				if err != nil {
					return opts, err
				}
				i = nextI
				opts.Chdir = v
			case "s3-cache-control":
				v, nextI, err := resolveValue(name, value, hasValue, args, i)
				if err != nil {
					return opts, err
				}
				i = nextI
				opts.S3CacheControl = strings.TrimSpace(v)
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
			case "gzip", "gunzip":
				opts.Compression = CompressionGzip
			case "bzip", "bzip2":
				opts.Compression = CompressionBzip2
			case "xz":
				opts.Compression = CompressionXz
			case "to-stdout":
				opts.ToStdout = true
			case "help":
				opts.Help = true
			case "progress":
				opts.Progress = ProgressAlways
			case "no-progress":
				opts.Progress = ProgressNever
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
	if err := validateOptions(opts); err != nil {
		return opts, err
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

var reservedSplitSuffixPattern = regexp.MustCompile(`(?i)^part[0-9]+$`)

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

// parseSplitSize parses a positive byte size with optional binary unit suffix.
func parseSplitSize(v string) (int64, error) {
	value := strings.TrimSpace(v)
	if value == "" {
		return 0, fmt.Errorf("option --split-size requires a positive byte size")
	}

	index := 0
	for index < len(value) && value[index] >= '0' && value[index] <= '9' {
		index++
	}
	numberText := value[:index]
	unitText := strings.ToUpper(strings.TrimSpace(value[index:]))
	number, err := strconv.ParseInt(numberText, 10, 64)
	if err != nil || number <= 0 {
		return 0, fmt.Errorf("option --split-size requires a positive byte size")
	}

	multiplier, ok := splitSizeUnits[unitText]
	if !ok {
		return 0, fmt.Errorf("option --split-size requires a positive byte size")
	}
	if number > math.MaxInt64/multiplier {
		return 0, fmt.Errorf("option --split-size value is too large")
	}
	return number * multiplier, nil
}

var splitSizeUnits = map[string]int64{
	"":    1,
	"B":   1,
	"K":   1024,
	"M":   1024 * 1024,
	"G":   1024 * 1024 * 1024,
	"T":   1024 * 1024 * 1024 * 1024,
	"KIB": 1024,
	"MIB": 1024 * 1024,
	"GIB": 1024 * 1024 * 1024,
	"TIB": 1024 * 1024 * 1024 * 1024,
}

// validateOptions performs cross-field validation after flag parsing.
func validateOptions(opts Options) error {
	if opts.Suffix != "" && reservedSplitSuffixPattern.MatchString(opts.Suffix) {
		return fmt.Errorf("option --suffix cannot use reserved split name %q", opts.Suffix)
	}
	if opts.Mode == ModeNone || opts.Archive == "" {
		return nil
	}
	if opts.SplitSizeBytes <= 0 {
		return nil
	}
	if opts.Mode != ModeCreate {
		return fmt.Errorf("option --split-size is only supported in create mode")
	}

	ref, err := locator.ParseArchive(opts.Archive)
	if err != nil {
		return err
	}
	if ref.Kind == locator.KindStdio {
		return fmt.Errorf("option --split-size does not support -f -")
	}
	if archiveutil.HasZipHint(archiveutil.NameHint(ref)) {
		return fmt.Errorf("option --split-size does not support zip archives")
	}
	switch opts.Compression {
	case CompressionBzip2, CompressionXz:
		return fmt.Errorf("option --split-size does not support %s compression", opts.Compression)
	}
	if _, ok := archivepath.ParseSplit(archiveutil.NameHint(ref)); ok {
		return fmt.Errorf("option --split-size cannot use an archive name that already contains .partNNNN")
	}
	return nil
}
