package engine

import (
	"archive/tar"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/islishude/gotgz/internal/archive"
	"github.com/islishude/gotgz/internal/cli"
	"github.com/islishude/gotgz/internal/compress"
	"github.com/islishude/gotgz/internal/locator"
)

// runListTar lists archive members from a tar input stream.
func (r *Runner) runListTar(ctx context.Context, opts cli.Options, reporter *progressReporter, ar io.ReadCloser, info archiveReaderInfo) (int, error) {
	return r.scanTarArchiveFromReader(ctx, opts, reporter, info, ar, func(hdr *tar.Header, tr *tar.Reader) (int, error) {
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

// runExtractTar extracts archive members from a tar input stream.
func (r *Runner) runExtractTar(ctx context.Context, opts cli.Options, reporter *progressReporter, ar io.ReadCloser, info archiveReaderInfo) (int, error) {
	policy := resolvePolicy(opts)
	metadataPolicy := resolveMetadataPolicy(opts)

	if opts.ToStdout {
		return r.scanTarArchiveFromReader(ctx, opts, reporter, info, ar, func(hdr *tar.Header, tr *tar.Reader) (int, error) {
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

	target := opts.Chdir
	if target == "" {
		target = "."
	}
	parsedTarget, err := locator.ParseArchive(target)
	if err != nil {
		return 0, err
	}

	return r.scanTarArchiveFromReader(ctx, opts, reporter, info, ar, func(hdr *tar.Header, tr *tar.Reader) (int, error) {
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
		switch parsedTarget.Kind {
		case locator.KindS3:
			return r.extractToS3(ctx, parsedTarget, &effectiveHdr, tr, reporter)
		case locator.KindLocal, locator.KindStdio:
			return r.extractToLocal(ctx, parsedTarget.Path, &effectiveHdr, tr, policy, metadataPolicy)
		default:
			return 0, fmt.Errorf("unsupported extract target %q", target)
		}
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
	obj := locator.Ref{Kind: locator.KindS3, Bucket: target.Bucket, Key: locator.JoinS3Prefix(target.Key, name)}
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
		if err := r.s3.UploadStream(ctx, obj, io.LimitReader(tr, hdr.Size), meta); err != nil {
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
		if err := r.s3.UploadStream(ctx, obj, empty, meta); err != nil {
			return warnings, err
		}
	}
	return warnings, nil
}

// extractToLocal writes one tar entry under base according to extraction policy.
func (r *Runner) extractToLocal(ctx context.Context, base string, hdr *tar.Header, tr *tar.Reader, policy PermissionPolicy, metadataPolicy MetadataPolicy) (int, error) {
	target, err := safeJoin(base, hdr.Name)
	if err != nil {
		return 0, err
	}
	mode := fs.FileMode(hdr.Mode)
	if !policy.SamePerms {
		mode = mode &^ currentUmask()
	}

	switch hdr.Typeflag {
	case tar.TypeDir:
		if err := os.MkdirAll(target, mode.Perm()); err != nil {
			return 0, err
		}
	case tar.TypeReg:
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return 0, err
		}
		f, err := os.OpenFile(target, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, mode.Perm())
		if err != nil {
			return 0, err
		}
		_, err = copyWithContext(ctx, f, io.LimitReader(tr, hdr.Size))
		cerr := f.Close()
		if err != nil {
			return 0, err
		}
		if cerr != nil {
			return 0, cerr
		}
	case tar.TypeSymlink:
		if err := safeSymlinkTarget(base, target, hdr.Linkname); err != nil {
			return 0, err
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return 0, err
		}
		if err := os.Remove(target); err != nil && !errors.Is(err, os.ErrNotExist) {
			return 0, err
		}
		if err := os.Symlink(hdr.Linkname, target); err != nil {
			return 0, err
		}
	case tar.TypeLink:
		linkTarget, err := safeJoin(base, hdr.Linkname)
		if err != nil {
			return 0, err
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return 0, err
		}
		_ = os.Remove(target)
		if err := os.Link(linkTarget, target); err != nil {
			return 0, err
		}
	default:
		if _, err := copyWithContext(ctx, io.Discard, io.LimitReader(tr, hdr.Size)); err != nil {
			return 0, err
		}
		return 0, nil
	}

	if policy.SamePerms {
		_ = os.Chmod(target, fs.FileMode(hdr.Mode).Perm())
	}
	if policy.SameOwner {
		_ = os.Lchown(target, hdr.Uid, hdr.Gid)
	}
	if !hdr.ModTime.IsZero() {
		_ = os.Chtimes(target, hdr.ModTime, hdr.ModTime)
	}

	xattrs, acls := decodeMetadataForExtract(hdr, metadataPolicy)
	_ = archive.WritePathMetadata(target, xattrs, acls)
	return 0, nil
}

// scanTarArchiveFromReader scans a tar stream with optional compression.
func (r *Runner) scanTarArchiveFromReader(ctx context.Context, opts cli.Options, reporter *progressReporter, info archiveReaderInfo, ar io.ReadCloser, fn func(hdr *tar.Header, tr *tar.Reader) (int, error)) (int, error) {
	reporter.SetTotal(info.Size, info.SizeKnown)
	ar = newCountingReadCloser(ar, reporter)

	cr, _, err := compress.NewReader(ar, compress.FromString(string(opts.Compression)), opts.Archive, info.ContentType)
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
