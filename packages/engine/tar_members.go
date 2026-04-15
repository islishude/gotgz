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
	"github.com/islishude/gotgz/packages/archiveprogress"
	"github.com/islishude/gotgz/packages/archiveutil"
	"github.com/islishude/gotgz/packages/locator"
)

// addS3TarMember writes one S3 object as a regular tar member.
func (r *Runner) addS3TarMember(ctx context.Context, tw tarArchiveWriter, ref locator.Ref, verbose bool, reporter *archiveprogress.Reporter) (err error) {
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

// writeLocalTarRecord writes one local filesystem record into the tar stream.
func (r *Runner) writeLocalTarRecord(ctx context.Context, tw tarArchiveWriter, record localCreateRecord, st fs.FileInfo, verbose bool, metadataPolicy MetadataPolicy, reporter *archiveprogress.Reporter) (int, error) {
	mode := st.Mode()
	isSymlink := mode&os.ModeSymlink != 0
	archiveName := filepath.ToSlash(record.archiveName)

	linkname := ""
	if isSymlink {
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
	hdr.Name = archiveName
	hdr.Format = tar.FormatPAX

	warnings := 0
	needsMetadata := metadataPolicy.Xattrs || metadataPolicy.ACL
	if needsMetadata {
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
	if mode.IsRegular() {
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
