package engine

import (
	"archive/tar"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
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

	archiveRef, err := applyArchiveSuffix(archiveRef, opts.Suffix)
	if err != nil {
		return 0, err
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
	excludeMatcher := newCompiledPathMatcher(excludes)
	if err := r.configureCreateProgressReporter(ctx, opts, excludeMatcher, reporter); err != nil {
		return 0, err
	}

	return r.processCreateMembers(
		ctx,
		opts,
		excludeMatcher,
		func(ref locator.Ref) error {
			return r.addS3Member(ctx, tw, ref, opts.Verbose, reporter)
		},
		func(member string) (int, error) {
			return r.addLocalPath(ctx, tw, member, opts.Chdir, excludeMatcher, opts.Verbose, metadataPolicy, reporter)
		},
	)
}

// configureCreateProgressReporter populates create-mode totals only when
// progress output is active for the current run.
func (r *Runner) configureCreateProgressReporter(ctx context.Context, opts cli.Options, excludeMatcher *compiledPathMatcher, reporter *progressReporter) error {
	if reporter == nil || !reporter.enabled {
		return nil
	}

	totalBytes, known, err := r.estimateCreateInputBytes(ctx, opts, excludeMatcher)
	if err != nil {
		return err
	}
	reporter.SetTotal(totalBytes, known)
	return nil
}

// estimateCreateInputBytes pre-computes input bytes for create mode progress and ETA.
func (r *Runner) estimateCreateInputBytes(ctx context.Context, opts cli.Options, excludeMatcher *compiledPathMatcher) (int64, bool, error) {
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
			if matchExcludeWithMatcher(excludeMatcher, ref.Key) {
				continue
			}
			meta, err := r.storage.statS3Object(ctx, ref)
			if err != nil {
				// Cannot determine size for this S3 object; fall back to
				// unknown-total progress so the actual archive still proceeds.
				return total, false, nil
			}
			total += meta.Size
		case locator.KindLocal:
			size, err := r.estimateLocalPathBytes(ctx, member, opts.Chdir, excludeMatcher)
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
func (r *Runner) estimateLocalPathBytes(ctx context.Context, member, chdir string, excludeMatcher *compiledPathMatcher) (int64, error) {
	var total int64

	err := walkLocalCreateMember(ctx, member, chdir, excludeMatcher, func(entry localCreateEntry) error {
		if entry.info.Mode().IsRegular() {
			total += entry.info.Size()
		}
		return nil
	})
	return total, err
}

// addS3Member writes one S3 object to the tar stream as a regular file member.
func (r *Runner) addS3Member(ctx context.Context, tw tarArchiveWriter, ref locator.Ref, verbose bool, reporter *progressReporter) (err error) {
	return r.streamS3MemberToArchive(ctx, ref, verbose, reporter, func(name string, size int64, modified time.Time, body io.Reader) error {
		hdr := &tar.Header{
			Name:     name,
			Mode:     0o644,
			Size:     size,
			Typeflag: tar.TypeReg,
			ModTime:  modified,
			Format:   tar.FormatPAX,
		}
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		if _, err := copyWithContext(ctx, tw, body); err != nil {
			return err
		}
		return tw.FinishEntry()
	})
}

// addLocalPath walks one local member path and writes entries into the tar
// stream, returning any metadata warnings emitted along the way.
func (r *Runner) addLocalPath(ctx context.Context, tw tarArchiveWriter, member, chdir string, excludeMatcher *compiledPathMatcher, verbose bool, metadataPolicy MetadataPolicy, reporter *progressReporter) (int, error) {
	warnings := 0
	err := walkLocalCreateMember(ctx, member, chdir, excludeMatcher, func(entry localCreateEntry) error {
		st := entry.info
		linkname := ""
		if st.Mode()&os.ModeSymlink != 0 {
			resolvedLink, err := os.Readlink(entry.current)
			if err != nil {
				return err
			}
			linkname = resolvedLink
		}
		hdr, err := tar.FileInfoHeader(st, linkname)
		if err != nil {
			return err
		}
		hdr.Name = filepath.ToSlash(entry.archiveName)
		hdr.Format = tar.FormatPAX

		if metadataPolicy.Xattrs || metadataPolicy.ACL {
			xattrs, acls, err := archive.ReadPathMetadata(entry.current)
			if err != nil {
				warnings += r.warnf(reporter, "create: metadata for %s is incomplete: %v", entry.current, err)
			}
			xattrs, acls = prepareMetadataForArchive(xattrs, acls, metadataPolicy)
			archive.EncodeXattrToPAX(hdr, xattrs)
			archive.EncodeACLToPAX(hdr, acls)
		}

		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		if st.Mode().IsRegular() {
			f, err := os.Open(entry.current)
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
	return warnings, err
}
