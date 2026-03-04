package engine

import (
	"archive/zip"
	"compress/flate"
	"context"
	"fmt"
	"io"
	"time"

	"github.com/islishude/gotgz/internal/cli"
	"github.com/islishude/gotgz/internal/locator"
)

// runCreateZip writes create-mode output in zip format.
func (r *Runner) runCreateZip(ctx context.Context, opts cli.Options, archiveRef locator.Ref) (warnings int, retErr error) {
	reporter := newProgressReporter(r.stderr, opts.Progress, 0, false, time.Now(), opts.Verbose)
	defer reporter.Finish()

	if opts.Suffix != "" {
		switch archiveRef.Kind {
		case locator.KindLocal:
			archiveRef.Path = AddArchiveSuffix(archiveRef.Path, opts.Suffix)
			archiveRef.Raw = archiveRef.Path
		case locator.KindS3:
			archiveRef.Key = AddArchiveSuffix(archiveRef.Key, opts.Suffix)
		case locator.KindStdio:
			return 0, fmt.Errorf("cannot use -suffix with -f -")
		}
	}

	warnings += r.warnZipCreateOptions(opts, reporter)

	aw, err := r.openArchiveWriter(ctx, archiveRef)
	if err != nil {
		return warnings, err
	}
	defer func() {
		if cerr := aw.Close(); cerr != nil && retErr == nil {
			retErr = fmt.Errorf("closing archive: %w", cerr)
		}
	}()

	zw := zip.NewWriter(aw)
	defer func() {
		if cerr := zw.Close(); cerr != nil && retErr == nil {
			retErr = fmt.Errorf("closing zip writer: %w", cerr)
		}
	}()

	if opts.CompressionLevel != nil {
		level := *opts.CompressionLevel
		zw.RegisterCompressor(zip.Deflate, func(dst io.Writer) (io.WriteCloser, error) {
			return flate.NewWriter(dst, level)
		})
	}

	excludes, err := loadExcludePatterns(opts.Exclude, opts.ExcludeFrom)
	if err != nil {
		return warnings, err
	}
	totalBytes, known, err := r.estimateCreateInputBytes(ctx, opts, excludes)
	if err != nil {
		return warnings, err
	}
	reporter.SetTotal(totalBytes, known)

	for _, member := range opts.Members {
		select {
		case <-ctx.Done():
			return warnings, ctx.Err()
		default:
		}
		ref, err := locator.ParseMember(member)
		if err != nil {
			return warnings, err
		}
		switch ref.Kind {
		case locator.KindS3:
			if matchExclude(excludes, ref.Key) {
				continue
			}
			if err := r.addS3MemberZip(ctx, zw, ref, opts.Verbose, reporter); err != nil {
				return warnings, err
			}
		case locator.KindLocal:
			w, err := r.addLocalPathZip(ctx, zw, member, opts.Chdir, excludes, opts.Verbose, reporter)
			warnings += w
			if err != nil {
				return warnings, err
			}
		default:
			return warnings, fmt.Errorf("unsupported member reference %q", member)
		}
	}
	return warnings, nil
}

// runListZip lists archive members from a zip input stream.
func (r *Runner) runListZip(ctx context.Context, opts cli.Options, reporter *progressReporter, archiveRef locator.Ref, ar io.ReadCloser, info archiveReaderInfo) (int, error) {
	warnings := r.warnZipReadOptions(opts, reporter)
	reporter.SetTotal(info.Size, info.SizeKnown)
	zipWarnings, err := r.withZipReader(ctx, archiveRef, ar, info, reporter, func(zr *zip.Reader) (int, error) {
		innerWarnings := 0
		for _, zf := range zr.File {
			select {
			case <-ctx.Done():
				return innerWarnings, ctx.Err()
			default:
			}
			if shouldSkipMember(opts, zf.Name) {
				continue
			}
			reporter.beforeExternalLineOutput()
			_, _ = fmt.Fprintln(r.stdout, zf.Name)
			reporter.afterExternalLineOutput()
		}
		return innerWarnings, nil
	})
	return warnings + zipWarnings, err
}

// runExtractZip extracts archive members from a zip input stream.
func (r *Runner) runExtractZip(ctx context.Context, opts cli.Options, reporter *progressReporter, archiveRef locator.Ref, ar io.ReadCloser, info archiveReaderInfo) (int, error) {
	policy := resolvePolicy(opts)
	warnings := r.warnZipReadOptions(opts, reporter)

	if opts.ToStdout {
		zipWarnings, err := r.withZipReader(ctx, archiveRef, ar, info, nil, func(zr *zip.Reader) (int, error) {
			total := totalZipPayloadBytes(zr, func(zf *zip.File) bool {
				if shouldSkipMember(opts, zf.Name) {
					return false
				}
				name, ok := stripPathComponents(zf.Name, opts.StripComponents)
				if !ok {
					return false
				}
				return name != "" && isZipRegular(zf)
			})
			reporter.SetTotal(total, true)
			return r.extractZipToStdout(zr, opts, reporter)
		})
		return warnings + zipWarnings, err
	}

	target := opts.Chdir
	if target == "" {
		target = "."
	}
	parsedTarget, err := locator.ParseArchive(target)
	if err != nil {
		return warnings, err
	}

	zipWarnings, err := r.withZipReader(ctx, archiveRef, ar, info, nil, func(zr *zip.Reader) (int, error) {
		total := totalZipPayloadBytes(zr, func(zf *zip.File) bool {
			if shouldSkipMember(opts, zf.Name) {
				return false
			}
			name, ok := stripPathComponents(zf.Name, opts.StripComponents)
			return ok && name != ""
		})
		reporter.SetTotal(total, true)

		innerWarnings := 0
		for _, zf := range zr.File {
			select {
			case <-ctx.Done():
				return innerWarnings, ctx.Err()
			default:
			}
			if shouldSkipMember(opts, zf.Name) {
				continue
			}
			extractName, ok := stripPathComponents(zf.Name, opts.StripComponents)
			if !ok || extractName == "" {
				continue
			}
			if opts.Verbose {
				reporter.beforeExternalLineOutput()
				_, _ = fmt.Fprintln(r.stdout, extractName)
				reporter.afterExternalLineOutput()
			}

			switch parsedTarget.Kind {
			case locator.KindS3:
				w, err := r.extractZipEntryToS3(ctx, parsedTarget, zf, extractName, reporter)
				innerWarnings += w
				if err != nil {
					return innerWarnings, err
				}
			case locator.KindLocal, locator.KindStdio:
				w, err := r.extractZipEntryToLocal(parsedTarget.Path, zf, extractName, policy, reporter)
				innerWarnings += w
				if err != nil {
					return innerWarnings, err
				}
			default:
				return innerWarnings, fmt.Errorf("unsupported extract target %q", target)
			}
		}
		return innerWarnings, nil
	})
	return warnings + zipWarnings, err
}
