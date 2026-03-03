package engine

import (
	"archive/zip"
	"compress/flate"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
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
			archiveRef.Path = AddTarSuffix(archiveRef.Path, opts.Suffix)
			archiveRef.Raw = archiveRef.Path
		case locator.KindS3:
			archiveRef.Key = AddTarSuffix(archiveRef.Key, opts.Suffix)
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

// addS3MemberZip writes one S3 object as a regular zip member.
func (r *Runner) addS3MemberZip(ctx context.Context, zw *zip.Writer, ref locator.Ref, verbose bool, reporter *progressReporter) (err error) {
	if strings.TrimSpace(ref.Key) == "" {
		return fmt.Errorf("s3 member key cannot be empty: %q", ref.Raw)
	}
	body, _, err := r.s3.OpenReader(ctx, ref)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := body.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()

	hdr := &zip.FileHeader{
		Name:   filepath.ToSlash(ref.Key),
		Method: zip.Deflate,
	}
	hdr.SetMode(0o644)
	hdr.Modified = time.Now()

	w, err := zw.CreateHeader(hdr)
	if err != nil {
		return err
	}
	if _, err := io.Copy(w, newCountingReader(body, reporter)); err != nil {
		return err
	}
	if verbose {
		reporter.beforeExternalLineOutput()
		_, _ = fmt.Fprintln(r.stdout, hdr.Name)
		reporter.afterExternalLineOutput()
	}
	return nil
}

// addLocalPathZip walks a local member and writes entries into the zip archive.
func (r *Runner) addLocalPathZip(ctx context.Context, zw *zip.Writer, member, chdir string, excludes []string, verbose bool, reporter *progressReporter) (int, error) {
	basePath := member
	if chdir != "" {
		basePath = filepath.Join(chdir, member)
	}
	cleanMember := path.Clean(filepath.ToSlash(member))
	warnings := 0
	err := filepath.WalkDir(basePath, func(current string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		rel, err := filepath.Rel(basePath, current)
		if err != nil {
			return err
		}
		archiveName := cleanMember
		if rel != "." {
			archiveName = path.Join(cleanMember, filepath.ToSlash(rel))
		}
		if matchExclude(excludes, archiveName) {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}

		st, err := os.Lstat(current)
		if err != nil {
			return err
		}
		entryName := filepath.ToSlash(archiveName)

		hdr, err := zip.FileInfoHeader(st)
		if err != nil {
			return err
		}
		hdr.Name = entryName
		if st.IsDir() {
			if !strings.HasSuffix(hdr.Name, "/") {
				hdr.Name += "/"
			}
			hdr.Method = zip.Store
		} else if st.Mode()&os.ModeSymlink != 0 {
			hdr.Method = zip.Store
		} else {
			hdr.Method = zip.Deflate
		}
		hdr.Modified = st.ModTime()
		hdr.SetMode(st.Mode())

		w, err := zw.CreateHeader(hdr)
		if err != nil {
			return err
		}
		switch {
		case st.IsDir():
		case st.Mode()&os.ModeSymlink != 0:
			linkTarget, err := os.Readlink(current)
			if err != nil {
				return err
			}
			if _, err := io.WriteString(w, linkTarget); err != nil {
				return err
			}
		case st.Mode().IsRegular():
			f, err := os.Open(current)
			if err != nil {
				return err
			}
			_, err = io.Copy(w, newCountingReader(f, reporter))
			cerr := f.Close()
			if err != nil {
				return err
			}
			if cerr != nil {
				return cerr
			}
		default:
			warnings += r.warnf(reporter, "zip create: unsupported local member type %s for %s; skipping payload", st.Mode().String(), current)
			return nil
		}

		if verbose {
			reporter.beforeExternalLineOutput()
			_, _ = fmt.Fprintln(r.stdout, hdr.Name)
			reporter.afterExternalLineOutput()
		}
		return nil
	})
	return warnings, err
}

// runListZip lists archive members from a zip input stream.
func (r *Runner) runListZip(ctx context.Context, opts cli.Options, reporter *progressReporter, archiveRef locator.Ref, ar io.ReadCloser, info archiveReaderInfo) (int, error) {
	warnings := r.warnZipReadOptions(opts, reporter)
	zipWarnings, err := r.withZipReader(archiveRef, ar, info, func(zr *zip.Reader) (int, error) {
		total := totalZipPayloadBytes(zr, func(zf *zip.File) bool {
			return !shouldSkipMember(opts, zf.Name)
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
			reporter.beforeExternalLineOutput()
			_, _ = fmt.Fprintln(r.stdout, zf.Name)
			reporter.afterExternalLineOutput()

			if isZipDir(zf) {
				continue
			}
			rc, w, err := r.openZipEntry(zf, reporter)
			innerWarnings += w
			if err != nil {
				return innerWarnings, err
			}
			if rc == nil {
				continue
			}
			_, err = io.Copy(io.Discard, newCountingReader(rc, reporter))
			cerr := rc.Close()
			if err != nil {
				return innerWarnings, err
			}
			if cerr != nil {
				return innerWarnings, cerr
			}
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
		zipWarnings, err := r.withZipReader(archiveRef, ar, info, func(zr *zip.Reader) (int, error) {
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

	zipWarnings, err := r.withZipReader(archiveRef, ar, info, func(zr *zip.Reader) (int, error) {
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

// withZipReader opens a zip.Reader from local file directly when possible and
// otherwise copies source bytes to a temporary file to satisfy ReaderAt.
func (r *Runner) withZipReader(archiveRef locator.Ref, ar io.ReadCloser, info archiveReaderInfo, fn func(zr *zip.Reader) (int, error)) (int, error) {
	if archiveRef.Kind == locator.KindLocal && info.SizeKnown && archiveRef.Path != "" {
		f, err := os.Open(archiveRef.Path)
		if err == nil {
			defer f.Close() //nolint:errcheck
			st, statErr := f.Stat()
			if statErr == nil && st.Mode().IsRegular() {
				zr, zipErr := zip.NewReader(f, st.Size())
				if zipErr == nil {
					return fn(zr)
				}
			}
		}
	}

	tmp, err := os.CreateTemp("", "gotgz-zip-*")
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
	}()

	if _, err := io.Copy(tmp, ar); err != nil {
		return 0, err
	}
	st, err := tmp.Stat()
	if err != nil {
		return 0, err
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}
	zr, err := zip.NewReader(tmp, st.Size())
	if err != nil {
		return 0, err
	}
	return fn(zr)
}

// extractZipToStdout writes matching regular zip members to stdout.
func (r *Runner) extractZipToStdout(zr *zip.Reader, opts cli.Options, reporter *progressReporter) (int, error) {
	warnings := 0
	for _, zf := range zr.File {
		if shouldSkipMember(opts, zf.Name) {
			continue
		}
		name, ok := stripPathComponents(zf.Name, opts.StripComponents)
		if !ok || name == "" || !isZipRegular(zf) {
			continue
		}
		rc, w, err := r.openZipEntry(zf, reporter)
		warnings += w
		if err != nil {
			return warnings, err
		}
		if rc == nil {
			continue
		}
		_, err = io.Copy(r.stdout, newCountingReader(rc, reporter))
		cerr := rc.Close()
		if err != nil {
			return warnings, err
		}
		if cerr != nil {
			return warnings, cerr
		}
	}
	return warnings, nil
}

// extractZipEntryToLocal extracts one zip entry into the local filesystem.
func (r *Runner) extractZipEntryToLocal(base string, zf *zip.File, extractName string, policy PermissionPolicy, reporter *progressReporter) (int, error) {
	target, err := safeJoin(base, extractName)
	if err != nil {
		return 0, err
	}
	mode := zf.Mode()
	modTime := zf.Modified
	warnings := 0

	switch {
	case isZipDir(zf):
		perm := mode.Perm()
		if perm == 0 {
			perm = 0o755
		}
		if !policy.SamePerms {
			perm = perm &^ currentUmask()
		}
		if err := os.MkdirAll(target, perm); err != nil {
			return warnings, err
		}
	case isZipSymlink(zf):
		rc, w, err := r.openZipEntry(zf, reporter)
		warnings += w
		if err != nil {
			return warnings, err
		}
		if rc == nil {
			return warnings, nil
		}
		linkBytes, err := io.ReadAll(newCountingReader(rc, reporter))
		cerr := rc.Close()
		if err != nil {
			return warnings, err
		}
		if cerr != nil {
			return warnings, cerr
		}
		linkTarget := string(linkBytes)
		if err := safeSymlinkTarget(base, target, linkTarget); err != nil {
			return warnings, err
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return warnings, err
		}
		if err := os.Remove(target); err != nil && !errors.Is(err, os.ErrNotExist) {
			return warnings, err
		}
		if err := os.Symlink(linkTarget, target); err != nil {
			return warnings, err
		}
	case isZipRegular(zf):
		perm := mode.Perm()
		if perm == 0 {
			perm = 0o644
		}
		if !policy.SamePerms {
			perm = perm &^ currentUmask()
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return warnings, err
		}
		out, err := os.OpenFile(target, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, perm)
		if err != nil {
			return warnings, err
		}
		rc, w, err := r.openZipEntry(zf, reporter)
		warnings += w
		if err != nil {
			_ = out.Close()
			return warnings, err
		}
		if rc == nil {
			_ = out.Close()
			return warnings, nil
		}
		_, err = io.Copy(out, newCountingReader(rc, reporter))
		rerr := rc.Close()
		cerr := out.Close()
		if err != nil {
			return warnings, err
		}
		if rerr != nil {
			return warnings, rerr
		}
		if cerr != nil {
			return warnings, cerr
		}
	default:
		warnings += r.warnf(reporter, "zip entry %s has unsupported type %s; skipping", zf.Name, mode.String())
		return warnings, nil
	}

	if policy.SamePerms && !isZipSymlink(zf) {
		perm := mode.Perm()
		if perm != 0 {
			_ = os.Chmod(target, perm)
		}
	}
	if !modTime.IsZero() && !isZipSymlink(zf) {
		_ = os.Chtimes(target, modTime, modTime)
	}
	return warnings, nil
}

// extractZipEntryToS3 extracts one zip entry into an S3 target.
func (r *Runner) extractZipEntryToS3(ctx context.Context, target locator.Ref, zf *zip.File, extractName string, reporter *progressReporter) (int, error) {
	name := strings.TrimPrefix(extractName, "./")
	if name == "" {
		return 0, nil
	}
	if isZipDir(zf) {
		return 0, nil
	}

	obj := locator.Ref{
		Kind:     locator.KindS3,
		Bucket:   target.Bucket,
		Key:      locator.JoinS3Prefix(target.Key, name),
		Metadata: target.Metadata,
	}

	if isZipRegular(zf) {
		rc, w, err := r.openZipEntry(zf, reporter)
		if err != nil {
			return w, err
		}
		if rc == nil {
			return w, nil
		}
		defer rc.Close() //nolint:errcheck
		if err := r.s3.UploadStream(ctx, obj, newCountingReader(rc, reporter), target.Metadata); err != nil {
			return w, err
		}
		return w, nil
	}

	if isZipSymlink(zf) {
		rc, w, err := r.openZipEntry(zf, reporter)
		if err != nil {
			return w, err
		}
		if rc == nil {
			return w, nil
		}
		linkBytes, err := io.ReadAll(newCountingReader(rc, reporter))
		cerr := rc.Close()
		if err != nil {
			return w, err
		}
		if cerr != nil {
			return w, cerr
		}
		w += r.warnf(reporter, "zip symlink %s extracted to S3 as regular object", zf.Name)
		if err := r.s3.UploadStream(ctx, obj, strings.NewReader(string(linkBytes)), target.Metadata); err != nil {
			return w, err
		}
		return w, nil
	}

	warnings := r.warnf(reporter, "zip entry %s has unsupported type %s on S3 target; skipping", zf.Name, zf.Mode().String())
	return warnings, nil
}

// openZipEntry opens one zip file entry and downgrades unsupported algorithms
// into warnings so extraction/list can continue.
func (r *Runner) openZipEntry(zf *zip.File, reporter *progressReporter) (io.ReadCloser, int, error) {
	rc, err := zf.Open()
	if err == nil {
		return rc, 0, nil
	}
	if errors.Is(err, zip.ErrAlgorithm) {
		return nil, r.warnf(reporter, "zip entry %s uses unsupported algorithm/encryption; skipping", zf.Name), nil
	}
	return nil, 0, err
}

// totalZipPayloadBytes sums uncompressed payload bytes for matching entries.
func totalZipPayloadBytes(zr *zip.Reader, match func(zf *zip.File) bool) int64 {
	var total int64
	for _, zf := range zr.File {
		if match != nil && !match(zf) {
			continue
		}
		if isZipDir(zf) {
			continue
		}
		total += int64(zf.UncompressedSize64)
	}
	return total
}

// isZipDir reports whether a zip entry is a directory.
func isZipDir(zf *zip.File) bool {
	if zf == nil {
		return false
	}
	if strings.HasSuffix(zf.Name, "/") {
		return true
	}
	return zf.FileInfo().IsDir()
}

// isZipSymlink reports whether a zip entry is a symbolic link.
func isZipSymlink(zf *zip.File) bool {
	if zf == nil {
		return false
	}
	return zf.Mode()&os.ModeSymlink != 0
}

// isZipRegular reports whether a zip entry should be treated as a regular file.
func isZipRegular(zf *zip.File) bool {
	if zf == nil || isZipDir(zf) || isZipSymlink(zf) {
		return false
	}
	return zf.Mode().IsRegular()
}

// warnZipCreateOptions emits warnings for create flags that do not apply to zip.
func (r *Runner) warnZipCreateOptions(opts cli.Options, reporter *progressReporter) int {
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
