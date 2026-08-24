package engine

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/islishude/gotgz/packages/archive"
	"github.com/islishude/gotgz/packages/archivepath"
	"github.com/islishude/gotgz/packages/archiveutil"
)

// computeExtractPerm normalizes extracted file permissions with optional fallbacks.
func computeExtractPerm(mode fs.FileMode, fallback fs.FileMode, samePerms bool, umasks ...fs.FileMode) fs.FileMode {
	perm := mode.Perm()
	if perm == 0 {
		perm = fallback
	}
	if !samePerms {
		umask := archive.CurrentUmask()
		if len(umasks) > 0 {
			umask = umasks[0]
		}
		perm &^= umask
	}
	return perm
}

// ensureLocalDirTarget creates one directory extraction target after path checks.
func ensureLocalDirTarget(base string, target string, perm fs.FileMode, cache *archivepath.PathSafetyCache) error {
	if err := archivepath.EnsureSymlinkFreePath(base, target, cache); err != nil {
		return err
	}
	temporaryPerm := perm | 0o700
	if err := os.MkdirAll(target, temporaryPerm); err != nil {
		return err
	}
	return os.Chmod(target, temporaryPerm)
}

// writeLocalRegularTarget writes one regular file extraction target after path checks.
func writeLocalRegularTarget(ctx context.Context, base string, target string, perm fs.FileMode, body io.Reader, cache *archivepath.PathSafetyCache) error {
	if err := archivepath.EnsureSymlinkFreePath(base, target, cache); err != nil {
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
func replaceLocalSymlinkTarget(base string, target string, linkname string, cache *archivepath.PathSafetyCache) error {
	if err := archivepath.EnsureSymlinkFreeParentPath(base, target, cache); err != nil {
		return err
	}
	if err := archivepath.SafeSymlinkTarget(base, target, linkname, cache); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return err
	}
	if err := os.Remove(target); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	cache.Invalidate(target)
	return os.Symlink(linkname, target)
}

// replaceLocalHardlinkTarget replaces one path with a validated hardlink target.
func replaceLocalHardlinkTarget(base string, target string, linkname string, cache *archivepath.PathSafetyCache) error {
	if err := archivepath.EnsureSymlinkFreeParentPath(base, target, cache); err != nil {
		return err
	}
	linkTarget, err := archivepath.SafeJoin(base, linkname)
	if err != nil {
		return err
	}
	if err := archivepath.EnsureSymlinkFreePath(base, linkTarget, cache); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return err
	}
	_ = os.Remove(target)
	return os.Link(linkTarget, target)
}
