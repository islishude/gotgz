package engine

import (
	"fmt"

	"github.com/islishude/gotgz/packages/cli"
)

// warnZipCreateOptions emits warnings for create flags that do not apply to zip.
func (r *Runner) warnZipCreateOptions(opts cli.Options, reporter *progressReporter) int {
	warnings := 0
	if opts.Xattrs {
		warnings += r.warnf(reporter, "--xattrs is not supported for zip archives and will be ignored")
	}
	if opts.ACL {
		warnings += r.warnf(reporter, "--acl is not supported for zip archives and will be ignored")
	}
	return warnings
}

// warnZipReadOptions emits warnings for read-time flags that do not apply to zip.
func (r *Runner) warnZipReadOptions(opts cli.Options, reporter *progressReporter) int {
	warnings := 0
	compression := normalizeCompressionHint(opts.Compression)
	if compression != cli.CompressionAuto && compression != cli.CompressionNone {
		warnings += r.warnf(reporter, "compression flags are ignored for zip archives")
	}
	if opts.Xattrs {
		warnings += r.warnf(reporter, "--xattrs is not supported for zip archives and will be ignored")
	}
	if opts.ACL {
		warnings += r.warnf(reporter, "--acl is not supported for zip archives and will be ignored")
	}
	if opts.SameOwner != nil {
		warnings += r.warnf(reporter, "--same-owner/--no-same-owner is not supported for zip archives and will be ignored")
	}
	if opts.NumericOwner {
		warnings += r.warnf(reporter, "--numeric-owner is not supported for zip archives and will be ignored")
	}
	return warnings
}

// normalizeCompressionHint converts the zero value into the parser default.
func normalizeCompressionHint(v cli.CompressionHint) cli.CompressionHint {
	if v == "" {
		return cli.CompressionAuto
	}
	return v
}

// warnf prints one warning and returns 1 for warning-count accumulation.
func (r *Runner) warnf(reporter *progressReporter, format string, args ...any) int {
	if reporter != nil {
		reporter.beforeExternalLineOutput()
	}
	_, _ = fmt.Fprintf(r.stderr, "gotgz: warning: "+format+"\n", args...)
	if reporter != nil {
		reporter.afterExternalLineOutput()
	}
	return 1
}
