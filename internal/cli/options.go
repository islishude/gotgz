package cli

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"

	"github.com/islishude/gotgz/internal/archivepath"
	"github.com/islishude/gotgz/internal/archiveutil"
	"github.com/islishude/gotgz/internal/compress"
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

// optionParser incrementally parses CLI arguments into Options.
type optionParser struct {
	args []string
	opts Options
}

// Parse converts argv-style CLI arguments into validated Options.
func Parse(args []string) (Options, error) {
	opts := defaultOptions()
	if len(args) == 0 {
		return opts, fmt.Errorf("no operation mode specified")
	}

	parser := optionParser{args: normalizeLegacyArgs(args), opts: opts}
	if err := parser.parse(); err != nil {
		return opts, err
	}
	return finalizeOptions(parser.opts)
}

// defaultOptions returns parser defaults before argv overrides are applied.
func defaultOptions() Options {
	return Options{Compression: CompressionAuto, Progress: ProgressAuto}
}

// normalizeLegacyArgs rewrites bundled GNU tar-style mode tokens into short flags.
func normalizeLegacyArgs(args []string) []string {
	if len(args) == 0 || !legacyToken(args[0]) {
		return args
	}
	return append([]string{"-" + args[0]}, args[1:]...)
}

// parse walks all argv tokens until options are exhausted or member parsing begins.
func (p *optionParser) parse() error {
	for i := 0; i < len(p.args); i++ {
		a := p.args[i]
		if a == "--" {
			p.opts.Members = append(p.opts.Members, p.args[i+1:]...)
			return nil
		}
		if consumed, err := p.parseCompatLongOption(i, a); err != nil {
			return err
		} else if consumed {
			i = p.nextIndex(i)
			continue
		}
		if !strings.HasPrefix(a, "-") || a == "-" {
			p.opts.Members = append(p.opts.Members, p.args[i:]...)
			return nil
		}
		if strings.HasPrefix(a, "--") {
			nextIndex, err := p.parseLongOption(i, a)
			if err != nil {
				return err
			}
			i = nextIndex
			continue
		}
		nextIndex, err := p.parseShortOptions(i, a)
		if err != nil {
			return err
		}
		i = nextIndex
	}
	return nil
}

// nextIndex advances the current argument index when a helper consumed an extra argv token.
func (p *optionParser) nextIndex(current int) int {
	return current + p.consumedValueArgs(current)
}

// consumedValueArgs reports whether the current token consumed the following argv entry as its value.
func (p *optionParser) consumedValueArgs(current int) int {
	if current < 0 || current >= len(p.args) {
		return 0
	}
	arg := p.args[current]
	if strings.Contains(arg, "=") {
		return 0
	}
	if strings.HasPrefix(arg, "--") {
		switch arg {
		case "--exclude", "--exclude-from", "--strip-components", "--compression-level", "--split-size", "--suffix", "--cd", "--directory", "--s3-cache-control":
			return 1
		default:
			return 0
		}
	}
	singleDashLong := strings.TrimPrefix(arg, "-")
	switch singleDashLong {
	case "compression-level", "suffix", "split-size":
		return 1
	default:
		return 0
	}
}

// parseCompatLongOption handles the historical single-dash long options kept for tar compatibility.
func (p *optionParser) parseCompatLongOption(i int, arg string) (bool, error) {
	if !strings.HasPrefix(arg, "-") || strings.HasPrefix(arg, "--") {
		return false, nil
	}
	for _, name := range []string{"compression-level", "suffix", "split-size"} {
		if !strings.HasPrefix(arg, "-"+name) {
			continue
		}
		parsedName, value, hasValue := strings.Cut(strings.TrimPrefix(arg, "-"), "=")
		if parsedName != name {
			return false, fmt.Errorf("unsupported option %s", arg)
		}
		_, err := p.applyLongOption(name, value, hasValue, i)
		return true, err
	}
	return false, nil
}

// parseLongOption parses one double-dash option and returns the next argv index to continue from.
func (p *optionParser) parseLongOption(i int, arg string) (int, error) {
	name, value, hasValue := strings.Cut(arg[2:], "=")
	nextIndex, err := p.applyLongOption(name, value, hasValue, i)
	if err != nil {
		return i, err
	}
	return nextIndex, nil
}

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
	case "progress":
		p.opts.Progress = ProgressAlways
	case "no-progress":
		p.opts.Progress = ProgressNever
	default:
		return i, fmt.Errorf("unsupported option --%s", name)
	}
	return i, nil
}

// parseShortOptions parses a single bundled short-option token.
func (p *optionParser) parseShortOptions(i int, arg string) (int, error) {
	shorts := arg[1:]
	for j := 0; j < len(shorts); j++ {
		s := shorts[j]
		switch s {
		case 'c':
			if err := setMode(&p.opts, ModeCreate); err != nil {
				return i, err
			}
		case 'x':
			if err := setMode(&p.opts, ModeExtract); err != nil {
				return i, err
			}
		case 't':
			if err := setMode(&p.opts, ModeList); err != nil {
				return i, err
			}
		case 'v':
			p.opts.Verbose = true
		case 'h':
			p.opts.Help = true
		case 'O':
			p.opts.ToStdout = true
		case 'z':
			p.opts.Compression = CompressionGzip
		case 'j':
			p.opts.Compression = CompressionBzip2
		case 'J':
			p.opts.Compression = CompressionXz
		case 'f', 'C':
			val, nextIndex, err := p.resolveShortValue(i, shorts, j, s)
			if err != nil {
				return i, err
			}
			if s == 'f' {
				p.opts.Archive = val
			} else {
				p.opts.Chdir = val
			}
			return nextIndex, nil
		default:
			return i, fmt.Errorf("unsupported option -%c", s)
		}
	}
	return i, nil
}

// resolveShortValue resolves short option values from inline suffixes or the next argv token.
func (p *optionParser) resolveShortValue(i int, shorts string, j int, option byte) (string, int, error) {
	if j+1 < len(shorts) {
		return shorts[j+1:], i, nil
	}
	nextIndex := i + 1
	if nextIndex >= len(p.args) {
		return "", i, fmt.Errorf("option -%c requires an argument", option)
	}
	return p.args[nextIndex], nextIndex, nil
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

// finalizeOptions applies post-parse validation and required-field checks.
func finalizeOptions(opts Options) (Options, error) {
	if opts.Help {
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
func validateOptions(opts Options) (Options, error) {
	if opts.Suffix != "" && reservedSplitSuffixPattern.MatchString(opts.Suffix) {
		return opts, fmt.Errorf("option --suffix cannot use reserved split name %q", opts.Suffix)
	}
	if opts.Mode == ModeNone || opts.Archive == "" {
		return opts, nil
	}

	ref, err := locator.ParseArchive(opts.Archive)
	if err != nil {
		return opts, err
	}
	if opts.Mode == ModeCreate {
		opts, err = normalizeCreateArchiveOutput(opts, ref)
		if err != nil {
			return opts, err
		}
	}
	if opts.SplitSizeBytes <= 0 {
		return opts, nil
	}
	if opts.Mode != ModeCreate {
		return opts, fmt.Errorf("option --split-size is only supported in create mode")
	}
	if ref.Kind == locator.KindStdio {
		return opts, fmt.Errorf("option --split-size does not support -f -")
	}
	if archiveutil.HasZipHint(archiveutil.NameHint(ref)) {
		return opts, fmt.Errorf("option --split-size does not support zip archives")
	}
	switch opts.Compression {
	case CompressionBzip2:
		return opts, fmt.Errorf("option --split-size does not support %s compression", opts.Compression)
	}
	if _, ok := archivepath.ParseSplit(archiveutil.NameHint(ref)); ok {
		return opts, fmt.Errorf("option --split-size cannot use an archive name that already contains .partNNNN")
	}
	return opts, nil
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
