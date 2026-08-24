package local

import (
	"fmt"
	"io/fs"
	"os"

	"github.com/islishude/gotgz/packages/archive"
)

type existingTargetMetadata struct {
	info     fs.FileInfo
	mode     fs.FileMode
	platform platformTargetMetadata
	xattrs   map[string][]byte
	acls     map[string][]byte
}

func captureExistingTargetMetadata(path string, info fs.FileInfo) (*existingTargetMetadata, error) {
	platform, err := inspectPlatformTargetMetadata(info)
	if err != nil {
		return nil, fmt.Errorf("inspect archive target metadata for %q: %w", path, err)
	}
	if platform.linkCount > 1 {
		return nil, fmt.Errorf("archive target %q has %d hard links; atomic overwrite requires a single-link target", path, platform.linkCount)
	}
	xattrs, acls, err := archive.ReadPathMetadata(path)
	if err != nil {
		return nil, fmt.Errorf("read archive target metadata for %q: %w", path, err)
	}
	mode := info.Mode() & (os.ModePerm | os.ModeSetuid | os.ModeSetgid | os.ModeSticky)
	return &existingTargetMetadata{
		info:     info,
		mode:     mode,
		platform: platform,
		xattrs:   xattrs,
		acls:     acls,
	}, nil
}

func (m *existingTargetMetadata) revalidate(path string) error {
	if m == nil {
		if _, err := os.Lstat(path); err == nil {
			return fmt.Errorf("archive target %q appeared during atomic write", path)
		} else if !os.IsNotExist(err) {
			return fmt.Errorf("revalidate new archive target %q: %w", path, err)
		}
		return nil
	}
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("revalidate archive target %q: %w", path, err)
	}
	if !info.Mode().IsRegular() || !os.SameFile(m.info, info) {
		return fmt.Errorf("archive target %q changed during atomic write", path)
	}
	platform, err := inspectPlatformTargetMetadata(info)
	if err != nil {
		return fmt.Errorf("revalidate archive target metadata for %q: %w", path, err)
	}
	if platform.linkCount > 1 {
		return fmt.Errorf("archive target %q acquired %d hard links during atomic write", path, platform.linkCount)
	}
	return nil
}

func (m *existingTargetMetadata) apply(file *os.File, tempPath string) error {
	if m == nil {
		return nil
	}
	if err := applyPlatformTargetMetadata(file, m.platform); err != nil {
		return fmt.Errorf("preserve archive target ownership: %w", err)
	}
	if err := archive.WritePathMetadata(tempPath, m.xattrs, m.acls); err != nil {
		return fmt.Errorf("preserve archive target xattrs and ACLs: %w", err)
	}
	if err := file.Chmod(m.mode); err != nil {
		return fmt.Errorf("preserve archive target mode: %w", err)
	}
	return nil
}
