package engine

import (
	"archive/zip"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/islishude/gotgz/internal/locator"
)

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
		linkTarget, err := readZipSymlinkTarget(zf, rc, reporter)
		cerr := rc.Close()
		if err != nil {
			return warnings, err
		}
		if cerr != nil {
			return warnings, cerr
		}
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
		rc, w, err := r.openZipEntry(zf, reporter)
		warnings += w
		if err != nil {
			return warnings, err
		}
		if rc == nil {
			return warnings, nil
		}

		perm := mode.Perm()
		if perm == 0 {
			perm = 0o644
		}
		if !policy.SamePerms {
			perm = perm &^ currentUmask()
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			_ = rc.Close()
			return warnings, err
		}
		out, err := os.OpenFile(target, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, perm)
		if err != nil {
			_ = rc.Close()
			return warnings, err
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
		w += r.warnf(reporter, "zip symlink %s extracted to S3 as regular object", zf.Name)
		err = r.s3.UploadStream(ctx, obj, newCountingReader(rc, reporter), target.Metadata)
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
