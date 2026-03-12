package engine

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/islishude/gotgz/packages/archiveutil"
)

// computeExtractPerm normalizes extracted file permissions with optional fallbacks.
func computeExtractPerm(mode fs.FileMode, fallback fs.FileMode, samePerms bool) fs.FileMode {
	perm := mode.Perm()
	if perm == 0 {
		perm = fallback
	}
	if !samePerms {
		perm &^= currentUmask()
	}
	return perm
}

// ensureLocalDirTarget creates one directory extraction target after path checks.
func ensureLocalDirTarget(base string, target string, perm fs.FileMode, cache *pathSafetyCache) error {
	if err := ensureSymlinkFreePath(base, target, cache); err != nil {
		return err
	}
	return os.MkdirAll(target, perm)
}

// writeLocalRegularTarget writes one regular file extraction target after path checks.
func writeLocalRegularTarget(ctx context.Context, base string, target string, perm fs.FileMode, body io.Reader, cache *pathSafetyCache) error {
	if err := ensureSymlinkFreePath(base, target, cache); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return err
	}
	file, err := os.OpenFile(target, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, perm)
	if err != nil {
		return err
	}
	_, err = archiveutil.CopyWithContext(ctx, file, body)
	closeErr := file.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	return nil
}

// replaceLocalSymlinkTarget replaces one path with a validated symlink target.
func replaceLocalSymlinkTarget(base string, target string, linkname string, cache *pathSafetyCache) error {
	if err := ensureSymlinkFreeParentPath(base, target, cache); err != nil {
		return err
	}
	if err := safeSymlinkTarget(base, target, linkname, cache); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return err
	}
	if err := os.Remove(target); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	cache.invalidate(target)
	return os.Symlink(linkname, target)
}

// replaceLocalHardlinkTarget replaces one path with a validated hardlink target.
func replaceLocalHardlinkTarget(base string, target string, linkname string, cache *pathSafetyCache) error {
	if err := ensureSymlinkFreeParentPath(base, target, cache); err != nil {
		return err
	}
	linkTarget, err := safeJoin(base, linkname)
	if err != nil {
		return err
	}
	if err := ensureSymlinkFreePath(base, linkTarget, cache); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return err
	}
	_ = os.Remove(target)
	return os.Link(linkTarget, target)
}

// applyLocalExtractMetadata restores file mode and timestamps when supported.
func applyLocalExtractMetadata(target string, mode fs.FileMode, modTime time.Time, samePerms bool, isSymlink bool) {
	if samePerms && !isSymlink {
		perm := mode.Perm()
		if perm != 0 {
			_ = os.Chmod(target, perm)
		}
	}
	if !modTime.IsZero() && !isSymlink {
		_ = os.Chtimes(target, modTime, modTime)
	}
}
