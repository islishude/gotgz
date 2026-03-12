package engine

import (
	"archive/tar"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/islishude/gotgz/packages/archive"
	"github.com/islishude/gotgz/packages/archivepath"
	"github.com/islishude/gotgz/packages/archiveprogress"
	"github.com/islishude/gotgz/packages/archiveutil"
	"github.com/islishude/gotgz/packages/cli"
	"github.com/islishude/gotgz/packages/locator"
)

// runCreateTar writes create-mode output in tar format.
func (r *Runner) runCreateTar(ctx context.Context, opts cli.Options, archiveRef locator.Ref) (warnings int, retErr error) {
	metadataPolicy := opts.ResolveMetadataPolicy()
	reporter := archiveprogress.NewReporter(r.stderr, opts.Progress, 0, false, time.Now(), opts.Verbose)
	defer reporter.Finish()

	archiveRef, err := archiveRef.WithArchiveSuffix(opts.Suffix)
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

	excludes, err := archivepath.LoadExcludePatterns(opts.Exclude, opts.ExcludeFrom)
	if err != nil {
		return 0, err
	}
	excludeMatcher := archivepath.NewCompiledPathMatcher(excludes)
	plan, err := r.buildCreatePlanIfEnabled(ctx, opts, excludeMatcher, reporter)
	if err != nil {
		return 0, err
	}
	if plan != nil {
		return r.processCreatePlan(
			ctx,
			plan,
			func(ref locator.Ref) error {
				return r.addS3Member(ctx, tw, ref, opts.Verbose, reporter)
			},
			func(entries []localCreateEntry) (int, error) {
				return r.addLocalEntries(ctx, tw, entries, opts.Verbose, metadataPolicy, reporter)
			},
		)
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

// addS3Member writes one S3 object to the tar stream as a regular file member.
func (r *Runner) addS3Member(ctx context.Context, tw tarArchiveWriter, ref locator.Ref, verbose bool, reporter *archiveprogress.Reporter) (err error) {
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
		if _, err := archiveutil.CopyWithContext(ctx, tw, body); err != nil {
			return err
		}
		return tw.FinishEntry()
	})
}

// addLocalPath walks one local member path and writes entries into the tar
// stream, returning any metadata warnings emitted along the way.
func (r *Runner) addLocalPath(ctx context.Context, tw tarArchiveWriter, member, chdir string, excludeMatcher *archivepath.CompiledPathMatcher, verbose bool, metadataPolicy MetadataPolicy, reporter *archiveprogress.Reporter) (int, error) {
	warnings := 0
	err := walkLocalCreateMember(ctx, member, chdir, excludeMatcher, func(entry localCreateEntry) error {
		w, err := r.writeLocalTarEntry(ctx, tw, entry, verbose, metadataPolicy, reporter)
		warnings += w
		return err
	})
	return warnings, err
}

// collectLocalCreateEntries walks one local member once and returns the
// normalized archive entries together with their total regular-file size.
func (r *Runner) collectLocalCreateEntries(ctx context.Context, member, chdir string, excludeMatcher *archivepath.CompiledPathMatcher) ([]localCreateEntry, int64, error) {
	entries := make([]localCreateEntry, 0)
	var total int64
	err := walkLocalCreateMember(ctx, member, chdir, excludeMatcher, func(entry localCreateEntry) error {
		entries = append(entries, entry)
		if entry.info.Mode().IsRegular() {
			total += entry.info.Size()
		}
		return nil
	})
	return entries, total, err
}

// addLocalEntries writes a pre-scanned set of local filesystem entries into the
// tar stream, returning any metadata warnings emitted along the way.
func (r *Runner) addLocalEntries(ctx context.Context, tw tarArchiveWriter, entries []localCreateEntry, verbose bool, metadataPolicy MetadataPolicy, reporter *archiveprogress.Reporter) (int, error) {
	warnings := 0
	for _, entry := range entries {
		select {
		case <-ctx.Done():
			return warnings, ctx.Err()
		default:
		}
		w, err := r.writeLocalTarEntry(ctx, tw, entry, verbose, metadataPolicy, reporter)
		warnings += w
		if err != nil {
			return warnings, err
		}
	}
	return warnings, nil
}

// writeLocalTarEntry writes one local filesystem entry into the tar stream.
func (r *Runner) writeLocalTarEntry(ctx context.Context, tw tarArchiveWriter, entry localCreateEntry, verbose bool, metadataPolicy MetadataPolicy, reporter *archiveprogress.Reporter) (int, error) {
	st := entry.info
	linkname := ""
	if st.Mode()&os.ModeSymlink != 0 {
		resolvedLink, err := os.Readlink(entry.current)
		if err != nil {
			return 0, err
		}
		linkname = resolvedLink
	}
	hdr, err := tar.FileInfoHeader(st, linkname)
	if err != nil {
		return 0, err
	}
	hdr.Name = filepath.ToSlash(entry.archiveName)
	hdr.Format = tar.FormatPAX

	warnings := 0
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
		return warnings, err
	}
	if st.Mode().IsRegular() {
		f, err := os.Open(entry.current)
		if err != nil {
			return warnings, err
		}
		_, err = archiveutil.CopyWithContext(ctx, tw, archiveprogress.NewCountingReader(f, reporter))
		cerr := f.Close()
		if err != nil {
			return warnings, err
		}
		if cerr != nil {
			return warnings, cerr
		}
	}
	if err := tw.FinishEntry(); err != nil {
		return warnings, err
	}
	if verbose {
		reporter.BeforeExternalLineOutput()
		_, _ = fmt.Fprintln(r.stdout, hdr.Name)
		reporter.AfterExternalLineOutput()
	}
	return warnings, nil
}
