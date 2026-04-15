package cli

import (
	"fmt"
	"strings"
)

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
		case "--exclude", "--exclude-from", "--strip-components", "--compression-level", "--split-size", "--suffix", "--cd", "--directory", "--s3-cache-control", "--s3-tag":
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
