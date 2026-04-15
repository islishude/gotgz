package engine

import (
	"archive/tar"
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"strings"

	"github.com/islishude/gotgz/packages/archive"
	"github.com/islishude/gotgz/packages/archivepath"
	"github.com/islishude/gotgz/packages/archiveprogress"
	"github.com/islishude/gotgz/packages/archiveutil"
	"github.com/islishude/gotgz/packages/locator"
)

// extractToS3 writes one tar entry into S3 while preserving supported metadata.
func (r *Runner) extractToS3(ctx context.Context, target locator.Ref, hdr *tar.Header, tr *tar.Reader, reporter *archiveprogress.Reporter) (int, error) {
	warnings := 0
	name := strings.TrimPrefix(hdr.Name, "./")
	if name == "" {
		if hdr.Size > 0 {
			if _, err := archiveutil.CopyWithContext(ctx, io.Discard, io.LimitReader(tr, hdr.Size)); err != nil {
				return warnings, err
			}
		}
		// Do not create an S3 object when the entry name is empty.
		return warnings, nil
	}
	meta, ok := archive.HeaderToS3Metadata(hdr)
	meta = archiveutil.MergeMetadata(target.Metadata, meta)
	if !ok {
		warnings++
		reporter.BeforeExternalLineOutput()
		_, _ = fmt.Fprintf(r.stderr, "gotgz: warning: metadata exceeds S3 metadata limit for %s\n", hdr.Name)
		reporter.AfterExternalLineOutput()
	}

	switch hdr.Typeflag {
	case tar.TypeReg:
		if err := r.uploadToS3Target(ctx, target, name, io.LimitReader(tr, hdr.Size), meta); err != nil {
			return warnings, err
		}
	case tar.TypeDir:
		// S3 has no real directories. Still need to consume any data associated with this entry.
		if _, err := archiveutil.CopyWithContext(ctx, io.Discard, io.LimitReader(tr, hdr.Size)); err != nil {
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
func (r *Runner) extractToLocal(ctx context.Context, base string, hdr *tar.Header, tr *tar.Reader, policy PermissionPolicy, metadataPolicy MetadataPolicy, safetyCache *archivepath.PathSafetyCache, reporter *archiveprogress.Reporter) (int, error) {
	target, err := archivepath.SafeJoin(base, hdr.Name)
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
		if _, err := archiveutil.CopyWithContext(ctx, io.Discard, io.LimitReader(tr, hdr.Size)); err != nil {
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
