package cli

import (
	"fmt"
	"strings"
)

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
		case 'V':
			p.opts.Version = true
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

// legacyToken reports whether the first argv token is a bundled tar-style mode string.
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

// setMode records the requested archive operation while rejecting conflicting modes.
func setMode(opts *Options, mode Mode) error {
	if opts.Mode != ModeNone && opts.Mode != mode {
		return fmt.Errorf("multiple operation modes specified")
	}
	opts.Mode = mode
	return nil
}
