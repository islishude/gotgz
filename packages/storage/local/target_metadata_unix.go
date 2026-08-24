//go:build unix

package local

import (
	"fmt"
	"io/fs"
	"os"
	"syscall"
)

type platformTargetMetadata struct {
	uid       int
	gid       int
	linkCount uint64
}

func inspectPlatformTargetMetadata(info fs.FileInfo) (platformTargetMetadata, error) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat == nil {
		return platformTargetMetadata{}, fmt.Errorf("filesystem stat data is unavailable")
	}
	return platformTargetMetadata{
		uid:       int(stat.Uid),
		gid:       int(stat.Gid),
		linkCount: uint64(stat.Nlink),
	}, nil
}

func applyPlatformTargetMetadata(file *os.File, metadata platformTargetMetadata) error {
	return file.Chown(metadata.uid, metadata.gid)
}

func platformTargetMetadataSupported() bool { return true }
