package engine

import (
	"archive/tar"
	"context"
	"fmt"
	"io"
	"io/fs"
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
func (r *Runner) runCreateTar(ctx context.Context, opts cli.Options, archiveRef locator.Ref, reporter *archiveprogress.Reporter) (warnings int, retErr error) {
	metadataPolicy := opts.ResolveMetadataPolicy()

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
	source, err := r.newCreateInputSource(ctx, opts, excludeMatcher, reporter != nil && reporter.Enabled())
	if err != nil {
		return 0, err
	}
	total, totalKnown := source.Total()
	reporter.SetTotal(total, totalKnown)

	return source.Visit(
		ctx,
		func(ref locator.Ref) error {
			return r.addS3Member(ctx, tw, ref, opts.Verbose, reporter)
		},
		func(source localCreateSource) (int, error) {
			return r.addLocalTarSource(ctx, tw, source, opts.Verbose, metadataPolicy, reporter)
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

// addLocalTarSource writes one local create source into the tar stream,
// returning any metadata warnings emitted along the way.
func (r *Runner) addLocalTarSource(ctx context.Context, tw tarArchiveWriter, source localCreateSource, verbose bool, metadataPolicy MetadataPolicy, reporter *archiveprogress.Reporter) (int, error) {
	return visitLocalCreateSource(ctx, source, func(record localCreateRecord, info fs.FileInfo) (int, error) {
		return r.writeLocalTarRecord(ctx, tw, record, info, verbose, metadataPolicy, reporter)
	})
}

// writeLocalTarRecord writes one local filesystem record into the tar stream.
func (r *Runner) writeLocalTarRecord(ctx context.Context, tw tarArchiveWriter, record localCreateRecord, st fs.FileInfo, verbose bool, metadataPolicy MetadataPolicy, reporter *archiveprogress.Reporter) (int, error) {
	linkname := ""
	if st.Mode()&os.ModeSymlink != 0 {
		resolvedLink, err := os.Readlink(record.current)
		if err != nil {
			return 0, err
		}
		linkname = resolvedLink
	}
	hdr, err := tar.FileInfoHeader(st, linkname)
	if err != nil {
		return 0, err
	}
	hdr.Name = filepath.ToSlash(record.archiveName)
	hdr.Format = tar.FormatPAX

	warnings := 0
	if metadataPolicy.Xattrs || metadataPolicy.ACL {
		xattrs, acls, err := archive.ReadPathMetadata(record.current)
		if err != nil {
			warnings += r.warnf(reporter, "create: metadata for %s is incomplete: %v", record.current, err)
		}
		xattrs, acls = prepareMetadataForArchive(xattrs, acls, metadataPolicy)
		archive.EncodeXattrToPAX(hdr, xattrs)
		archive.EncodeACLToPAX(hdr, acls)
	}

	if err := tw.WriteHeader(hdr); err != nil {
		return warnings, err
	}
	if st.Mode().IsRegular() {
		f, err := os.Open(record.current)
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
