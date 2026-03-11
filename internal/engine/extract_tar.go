package engine

import (
	"archive/tar"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"strings"

	"github.com/islishude/gotgz/internal/archive"
	"github.com/islishude/gotgz/internal/archiveutil"
	"github.com/islishude/gotgz/internal/cli"
	"github.com/islishude/gotgz/internal/compress"
	"github.com/islishude/gotgz/internal/locator"
)

// runListTar lists archive members from a tar input stream or split-volume set.
func (r *Runner) runListTar(ctx context.Context, opts cli.Options, reporter *progressReporter, ref locator.Ref, ar io.ReadCloser, info archiveReaderInfo) (int, error) {
	volumes, err := r.resolveArchiveVolumes(ctx, ref, info)
	if err != nil {
		return 0, err
	}

	scan := func(scanReader io.ReadCloser, scanInfo archiveReaderInfo) (int, error) {
		return r.scanTarArchiveFromReader(ctx, opts, reporter, scanInfo, archiveutil.NameHint(ref), scanReader, func(hdr *tar.Header, tr *tar.Reader) (int, error) {
			if shouldSkipMember(opts, hdr.Name) {
				if _, err := copyWithContext(ctx, io.Discard, tr); err != nil {
					return 0, err
				}
				return 0, nil
			}
			reporter.beforeExternalLineOutput()
			_, _ = fmt.Fprintln(r.stdout, hdr.Name)
			reporter.afterExternalLineOutput()
			if _, err := copyWithContext(ctx, io.Discard, tr); err != nil {
				return 0, err
			}
			return 0, nil
		})
	}

	if len(volumes) == 1 {
		reporter.SetTotal(info.Size, info.SizeKnown)
		return scan(ar, info)
	}
	return r.scanTarArchiveFromVolumes(ctx, opts, reporter, volumes, ar, scan)
}

// runExtractTar extracts archive members from a tar input stream or split-volume set.
func (r *Runner) runExtractTar(ctx context.Context, opts cli.Options, reporter *progressReporter, ref locator.Ref, ar io.ReadCloser, info archiveReaderInfo) (int, error) {
	volumes, err := r.resolveArchiveVolumes(ctx, ref, info)
	if err != nil {
		return 0, err
	}

	scan := func(scanReader io.ReadCloser, scanInfo archiveReaderInfo) (int, error) {
		return r.runExtractTarReader(ctx, opts, reporter, scanReader, scanInfo)
	}

	if len(volumes) == 1 {
		reporter.SetTotal(info.Size, info.SizeKnown)
		return scan(ar, info)
	}
	return r.scanTarArchiveFromVolumes(ctx, opts, reporter, volumes, ar, scan)
}

// runExtractTarReader extracts archive members from a single tar volume reader.
func (r *Runner) runExtractTarReader(ctx context.Context, opts cli.Options, reporter *progressReporter, ar io.ReadCloser, info archiveReaderInfo) (int, error) {
	policy := resolvePolicy(opts)
	metadataPolicy := resolveMetadataPolicy(opts)

	if opts.ToStdout {
		return r.scanTarArchiveFromReader(ctx, opts, reporter, info, opts.Archive, ar, func(hdr *tar.Header, tr *tar.Reader) (int, error) {
			if shouldSkipMember(opts, hdr.Name) {
				if _, err := copyWithContext(ctx, io.Discard, tr); err != nil {
					return 0, err
				}
				return 0, nil
			}
			if _, ok := stripPathComponents(hdr.Name, opts.StripComponents); !ok {
				if _, err := copyWithContext(ctx, io.Discard, io.LimitReader(tr, hdr.Size)); err != nil {
					return 0, err
				}
				return 0, nil
			}
			if hdr.Typeflag != tar.TypeReg {
				if _, err := copyWithContext(ctx, io.Discard, tr); err != nil {
					return 0, err
				}
				return 0, nil
			}
			_, err := copyWithContext(ctx, r.stdout, tr)
			return 0, err
		})
	}

	parsedTarget, err := parseExtractTarget(opts.Chdir, opts.S3CacheControl)
	if err != nil {
		return 0, err
	}
	target := opts.Chdir
	if target == "" {
		target = "."
	}
	var safetyCache *pathSafetyCache
	if parsedTarget.Kind == locator.KindLocal || parsedTarget.Kind == locator.KindStdio {
		safetyCache = newPathSafetyCache()
	}

	return r.scanTarArchiveFromReader(ctx, opts, reporter, info, opts.Archive, ar, func(hdr *tar.Header, tr *tar.Reader) (int, error) {
		if shouldSkipMember(opts, hdr.Name) {
			if _, err := copyWithContext(ctx, io.Discard, tr); err != nil {
				return 0, err
			}
			return 0, nil
		}
		extractName, ok := stripPathComponents(hdr.Name, opts.StripComponents)
		if !ok {
			if _, err := copyWithContext(ctx, io.Discard, io.LimitReader(tr, hdr.Size)); err != nil {
				return 0, err
			}
			return 0, nil
		}
		effectiveHdr := *hdr
		effectiveHdr.Name = extractName
		if opts.Verbose {
			reporter.beforeExternalLineOutput()
			_, _ = fmt.Fprintln(r.stdout, effectiveHdr.Name)
			reporter.afterExternalLineOutput()
		}
		return r.dispatchExtractTarget(
			parsedTarget,
			target,
			func(target locator.Ref) (int, error) {
				return r.extractToS3(ctx, target, &effectiveHdr, tr, reporter)
			},
			func(base string) (int, error) {
				return r.extractToLocal(ctx, base, &effectiveHdr, tr, policy, metadataPolicy, safetyCache, reporter)
			},
		)
	})
}

// extractToS3 writes one tar entry into S3 while preserving supported metadata.
func (r *Runner) extractToS3(ctx context.Context, target locator.Ref, hdr *tar.Header, tr *tar.Reader, reporter *progressReporter) (int, error) {
	warnings := 0
	name := strings.TrimPrefix(hdr.Name, "./")
	if name == "" {
		if hdr.Size > 0 {
			if _, err := copyWithContext(ctx, io.Discard, io.LimitReader(tr, hdr.Size)); err != nil {
				return warnings, err
			}
		}
		// Do not create an S3 object when the entry name is empty.
		return warnings, nil
	}
	meta, ok := archive.HeaderToS3Metadata(hdr)
	meta = mergeMetadata(target.Metadata, meta)
	if !ok {
		warnings++
		reporter.beforeExternalLineOutput()
		_, _ = fmt.Fprintf(r.stderr, "gotgz: warning: metadata exceeds S3 metadata limit for %s\n", hdr.Name)
		reporter.afterExternalLineOutput()
	}

	switch hdr.Typeflag {
	case tar.TypeReg:
		if err := r.uploadToS3Target(ctx, target, name, io.LimitReader(tr, hdr.Size), meta); err != nil {
			return warnings, err
		}
	case tar.TypeDir:
		// S3 has no real directories. Still need to consume any data associated with this entry.
		if _, err := copyWithContext(ctx, io.Discard, io.LimitReader(tr, hdr.Size)); err != nil {
			return warnings, err
		}
	default:
		empty := strings.NewReader("")
		meta["gotgz-type"] = fmt.Sprintf("%d", hdr.Typeflag)
		if err := r.uploadToS3Target(ctx, target, name, empty, meta); err != nil {
			return warnings, err
		}
	}
	return warnings, nil
}

// extractToLocal writes one tar entry under base according to extraction policy.
func (r *Runner) extractToLocal(ctx context.Context, base string, hdr *tar.Header, tr *tar.Reader, policy PermissionPolicy, metadataPolicy MetadataPolicy, safetyCache *pathSafetyCache, reporter *progressReporter) (int, error) {
	target, err := safeJoin(base, hdr.Name)
	if err != nil {
		return 0, err
	}
	mode := fs.FileMode(hdr.Mode)
	extractPerm := computeExtractPerm(mode, 0, policy.SamePerms)
	warnings := 0

	switch hdr.Typeflag {
	case tar.TypeDir:
		if err := ensureLocalDirTarget(base, target, extractPerm, safetyCache); err != nil {
			return warnings, err
		}
	case tar.TypeReg:
		if err := writeLocalRegularTarget(ctx, base, target, extractPerm, io.LimitReader(tr, hdr.Size), safetyCache); err != nil {
			return warnings, err
		}
	case tar.TypeSymlink:
		if err := replaceLocalSymlinkTarget(base, target, hdr.Linkname, safetyCache); err != nil {
			return warnings, err
		}
	case tar.TypeLink:
		if err := replaceLocalHardlinkTarget(base, target, hdr.Linkname, safetyCache); err != nil {
			return warnings, err
		}
	default:
		if _, err := copyWithContext(ctx, io.Discard, io.LimitReader(tr, hdr.Size)); err != nil {
			return warnings, err
		}
		return warnings, nil
	}

	applyLocalExtractMetadata(target, mode, hdr.ModTime, policy.SamePerms, hdr.Typeflag == tar.TypeSymlink)
	if policy.SameOwner {
		_ = os.Lchown(target, hdr.Uid, hdr.Gid)
	}

	xattrs, acls, err := decodeMetadataForExtract(hdr, metadataPolicy)
	if err != nil {
		warnings += r.warnf(reporter, "extract: metadata for %s is malformed: %v", hdr.Name, err)
	}
	if err := archive.WritePathMetadata(target, xattrs, acls); err != nil {
		warnings += r.warnf(reporter, "extract: metadata for %s could not be fully restored: %v", hdr.Name, err)
	}
	return warnings, nil
}

// scanTarArchiveFromReader scans a tar stream with optional compression.
func (r *Runner) scanTarArchiveFromReader(ctx context.Context, opts cli.Options, reporter *progressReporter, info archiveReaderInfo, hint string, ar io.ReadCloser, fn func(hdr *tar.Header, tr *tar.Reader) (int, error)) (int, error) {
	return r.scanTarArchiveStream(ctx, opts, reporter, info, hint, ar, fn)
}

// scanTarArchiveFromVolumes scans a discovered split archive volume-by-volume.
func (r *Runner) scanTarArchiveFromVolumes(ctx context.Context, _ cli.Options, reporter *progressReporter, volumes []archiveVolume, first io.ReadCloser, scan func(io.ReadCloser, archiveReaderInfo) (int, error)) (int, error) {
	var total int64
	totalKnown := true
	for _, volume := range volumes {
		if !volume.info.SizeKnown {
			totalKnown = false
			continue
		}
		total += volume.info.Size
	}
	reporter.SetTotal(total, totalKnown)

	warnings := 0
	for index, volume := range volumes {
		var (
			reader io.ReadCloser
			info   archiveReaderInfo
			err    error
		)

		if index == 0 {
			reader = first
			info = volume.info
		} else {
			reader, info, err = r.openArchiveReader(ctx, volume.ref)
			if err != nil {
				return warnings, err
			}
			info = mergeArchiveReaderInfo(volume.info, info)
		}

		w, err := scan(reader, info)
		warnings += w
		if err != nil {
			return warnings, err
		}
	}
	return warnings, nil
}

// scanTarArchiveStream scans one tar stream with optional compression.
func (r *Runner) scanTarArchiveStream(ctx context.Context, opts cli.Options, reporter *progressReporter, info archiveReaderInfo, hint string, ar io.ReadCloser, fn func(hdr *tar.Header, tr *tar.Reader) (int, error)) (int, error) {
	ar = newCountingReadCloser(ar, reporter)

	cr, _, err := compress.NewReader(ar, compress.FromString(string(opts.Compression)), hint, info.ContentType)
	if err != nil {
		return 0, err
	}
	defer cr.Close() //nolint:errcheck

	tr := tar.NewReader(cr)
	warnings := 0
	for {
		select {
		case <-ctx.Done():
			return warnings, ctx.Err()
		default:
		}
		hdr, err := tr.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return warnings, err
		}
		select {
		case <-ctx.Done():
			return warnings, ctx.Err()
		default:
		}
		w, err := fn(hdr, tr)
		warnings += w
		if err != nil {
			return warnings, err
		}
	}
	return warnings, nil
}
