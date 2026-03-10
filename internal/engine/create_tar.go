package engine

import (
	"archive/tar"
	"context"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/islishude/gotgz/internal/archive"
	"github.com/islishude/gotgz/internal/cli"
	"github.com/islishude/gotgz/internal/locator"
)

// runCreateTar writes create-mode output in tar format.
func (r *Runner) runCreateTar(ctx context.Context, opts cli.Options, archiveRef locator.Ref) (warnings int, retErr error) {
	metadataPolicy := resolveMetadataPolicy(opts)
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
	tw, err := r.newTarArchiveWriter(ctx, opts, archiveRef)
	if err != nil {
		return 0, err
	}
	defer func() {
		if cerr := tw.Close(); cerr != nil && retErr == nil {
			retErr = cerr
		}
	}()

	excludes, err := loadExcludePatterns(opts.Exclude, opts.ExcludeFrom)
	if err != nil {
		return 0, err
	}
	totalBytes, known, err := r.estimateCreateInputBytes(ctx, opts, excludes)
	if err != nil {
		return 0, err
	}
	reporter.SetTotal(totalBytes, known)

	for _, m := range opts.Members {
		select {
		case <-ctx.Done():
			return warnings, ctx.Err()
		default:
		}
		ref, err := locator.ParseMember(m)
		if err != nil {
			return warnings, err
		}
		switch ref.Kind {
		case locator.KindS3:
			if matchExclude(excludes, ref.Key) {
				continue
			}
			if err := r.addS3Member(ctx, tw, ref, opts.Verbose, reporter); err != nil {
				return warnings, err
			}
		case locator.KindLocal:
			if err := r.addLocalPath(ctx, tw, m, opts.Chdir, excludes, opts.Verbose, metadataPolicy, reporter); err != nil {
				return warnings, err
			}
		default:
			return warnings, fmt.Errorf("unsupported member reference %q", m)
		}
	}
	return warnings, nil
}

// estimateCreateInputBytes pre-computes input bytes for create mode progress and ETA.
func (r *Runner) estimateCreateInputBytes(ctx context.Context, opts cli.Options, excludes []string) (int64, bool, error) {
	var total int64
	for _, member := range opts.Members {
		select {
		case <-ctx.Done():
			return 0, false, ctx.Err()
		default:
		}

		ref, err := locator.ParseMember(member)
		if err != nil {
			return 0, false, err
		}

		switch ref.Kind {
		case locator.KindS3:
			if matchExclude(excludes, ref.Key) {
				continue
			}
			meta, err := r.s3.Stat(ctx, ref)
			if err != nil {
				// Cannot determine size for this S3 object; fall back to
				// unknown-total progress so the actual archive still proceeds.
				return total, false, nil
			}
			total += meta.Size
		case locator.KindLocal:
			size, err := r.estimateLocalPathBytes(ctx, member, opts.Chdir, excludes)
			if err != nil {
				return 0, false, err
			}
			total += size
		default:
			return 0, false, fmt.Errorf("unsupported member reference %q", member)
		}
	}
	return total, true, nil
}

// estimateLocalPathBytes sums regular file bytes for one local create member.
func (r *Runner) estimateLocalPathBytes(ctx context.Context, member, chdir string, excludes []string) (int64, error) {
	basePath := member
	if chdir != "" {
		basePath = filepath.Join(chdir, member)
	}
	cleanMember := path.Clean(filepath.ToSlash(member))
	var total int64

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
		if st.Mode().IsRegular() {
			total += st.Size()
		}
		return nil
	})
	return total, err
}

// addS3Member writes one S3 object to the tar stream as a regular file member.
func (r *Runner) addS3Member(ctx context.Context, tw tarArchiveWriter, ref locator.Ref, verbose bool, reporter *progressReporter) (err error) {
	if strings.TrimSpace(ref.Key) == "" {
		return fmt.Errorf("s3 member key cannot be empty: %q", ref.Raw)
	}
	body, meta, err := r.s3.OpenReader(ctx, ref)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := body.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()

	hdr := &tar.Header{
		Name:     ref.Key,
		Mode:     0o644,
		Size:     meta.Size,
		Typeflag: tar.TypeReg,
		ModTime:  time.Now(),
		Format:   tar.FormatPAX,
	}
	if err := tw.WriteHeader(hdr); err != nil {
		return err
	}
	if _, err := copyWithContext(ctx, tw, newCountingReader(body, reporter)); err != nil {
		return err
	}
	if err := tw.FinishEntry(); err != nil {
		return err
	}
	if verbose {
		reporter.beforeExternalLineOutput()
		_, _ = fmt.Fprintln(r.stdout, hdr.Name)
		reporter.afterExternalLineOutput()
	}
	return nil
}

// addLocalPath walks one local member path and writes entries into the tar stream.
func (r *Runner) addLocalPath(ctx context.Context, tw tarArchiveWriter, member, chdir string, excludes []string, verbose bool, metadataPolicy MetadataPolicy, reporter *progressReporter) error {
	basePath := member
	if chdir != "" {
		basePath = filepath.Join(chdir, member)
	}
	cleanMember := path.Clean(filepath.ToSlash(member))
	return filepath.WalkDir(basePath, func(current string, d fs.DirEntry, walkErr error) error {
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
		linkname := ""
		if st.Mode()&os.ModeSymlink != 0 {
			linkname, _ = os.Readlink(current)
		}
		hdr, err := tar.FileInfoHeader(st, linkname)
		if err != nil {
			return err
		}
		hdr.Name = filepath.ToSlash(archiveName)
		hdr.Format = tar.FormatPAX

		if metadataPolicy.Xattrs || metadataPolicy.ACL {
			xattrs, acls, _ := archive.ReadPathMetadata(current)
			xattrs, acls = prepareMetadataForArchive(xattrs, acls, metadataPolicy)
			archive.EncodeXattrToPAX(hdr, xattrs)
			archive.EncodeACLToPAX(hdr, acls)
		}

		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		if st.Mode().IsRegular() {
			f, err := os.Open(current)
			if err != nil {
				return err
			}
			_, err = copyWithContext(ctx, tw, newCountingReader(f, reporter))
			cerr := f.Close()
			if err != nil {
				return err
			}
			if cerr != nil {
				return cerr
			}
		}
		if err := tw.FinishEntry(); err != nil {
			return err
		}
		if verbose {
			reporter.beforeExternalLineOutput()
			_, _ = fmt.Fprintln(r.stdout, hdr.Name)
			reporter.afterExternalLineOutput()
		}
		return nil
	})
}
