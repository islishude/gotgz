package engine

import (
	"archive/tar"
	"context"
	"io"
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
func (r *Runner) writeLocalTarRecord(ctx context.Context, tw tarArchiveWriter, entry *localEntryHandle, verbose bool, metadataPolicy MetadataPolicy, reporter *archiveprogress.Reporter) (int, error) {
	record := entry.record
	st := entry.info
	mode := st.Mode()
	archiveName := filepath.ToSlash(record.archiveName)

	hdr, err := tar.FileInfoHeader(st, entry.linkTarget)
	if err != nil {
		return 0, err
	}
	hdr.Name = archiveName
	hdr.Format = tar.FormatPAX

	warnings := 0
	needsMetadata := metadataPolicy.Xattrs || metadataPolicy.ACL
	if needsMetadata {
		// Extended metadata remains path-based even when Linux payload reads use
		// an opened fd, so a concurrent path replacement can still race here.
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
		if err := copyLocalEntryPayload(ctx, tw, entry, reporter); err != nil {
			return warnings, err
		}
	}
	if err := tw.FinishEntry(); err != nil {
		return warnings, err
	}
	if verbose {
		reporter.ExternalLinef(r.stdout, "%s\n", hdr.Name)
	}
	return warnings, nil
}
