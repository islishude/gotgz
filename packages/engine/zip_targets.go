package engine

import (
	"archive/zip"
	"context"
	"strings"

	"github.com/islishude/gotgz/packages/archivepath"
	"github.com/islishude/gotgz/packages/archiveprogress"
	"github.com/islishude/gotgz/packages/locator"
)

// extractZipEntryToLocal extracts one zip entry into the local filesystem.
func (r *Runner) extractZipEntryToLocal(ctx context.Context, base string, zf *zip.File, extractName string, policy PermissionPolicy, safetyCache *archivepath.PathSafetyCache, metadataSession *localMetadataSession, reporter *archiveprogress.Reporter) (int, error) {
	target, err := archivepath.SafeJoin(base, extractName)
	if err != nil {
		return 0, err
	}
	mode := zf.Mode()
	modTime := zf.Modified
	warnings := 0
	perm := computeExtractPerm(mode, 0o644, policy.SamePerms, policy.Umask)

	switch {
	case isZipDir(zf):
		perm = computeExtractPerm(mode, 0o755, policy.SamePerms, policy.Umask)
		if err := ensureLocalDirTarget(base, target, perm, safetyCache); err != nil {
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
		linkTarget, err := readZipSymlinkTarget(zf, rc, reporter)
		cerr := rc.Close()
		if err != nil {
			return warnings, err
		}
		if cerr != nil {
			return warnings, cerr
		}
		if err := replaceLocalSymlinkTarget(base, target, linkTarget, safetyCache); err != nil {
			return warnings, err
		}
		metadataSession.discardDirectory(target)
	case isZipRegular(zf):
		rc, w, err := r.openZipEntry(zf, reporter)
		warnings += w
		if err != nil {
			return warnings, err
		}
		if rc == nil {
			return warnings, nil
		}
		err = writeLocalRegularTarget(ctx, base, target, perm, archiveprogress.NewCountingReader(rc, reporter), safetyCache)
		rerr := rc.Close()
		if err != nil {
			return warnings, err
		}
		if rerr != nil {
			return warnings, rerr
		}
	default:
		warnings += r.warnf(reporter, "zip entry %s has unsupported type %s; skipping", zf.Name, mode.String())
		return warnings, nil
	}

	record := localMetadataRecord{
		target:      target,
		archiveName: extractName,
		perm:        perm,
		modTime:     modTime,
		isSymlink:   isZipSymlink(zf),
	}
	if isZipDir(zf) {
		metadataSession.queueDirectory(record)
	} else {
		warnings += metadataSession.apply(record)
	}
	return warnings, nil
}

// extractZipEntryToS3 extracts one zip entry into an S3 target.
func (r *Runner) extractZipEntryToS3(ctx context.Context, target locator.Ref, zf *zip.File, extractName string, reporter *archiveprogress.Reporter) (int, error) {
	name := strings.TrimPrefix(extractName, "./")
	if name == "" {
		return 0, nil
	}
	if isZipDir(zf) {
		return 0, nil
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
		if err := r.uploadToS3Target(ctx, target, name, archiveprogress.NewCountingReader(rc, reporter), target.Metadata); err != nil {
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
		w += r.warnf(reporter, "zip symlink %s extracted to S3 as regular object", zf.Name)
		err = r.uploadToS3Target(ctx, target, name, archiveprogress.NewCountingReader(rc, reporter), target.Metadata)
		cerr := rc.Close()
		if err != nil {
			return w, err
		}
		if cerr != nil {
			return w, cerr
		}
		return w, nil
	}

	warnings := r.warnf(reporter, "zip entry %s has unsupported type %s on S3 target; skipping", zf.Name, zf.Mode().String())
	return warnings, nil
}
