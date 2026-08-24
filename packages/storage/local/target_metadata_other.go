//go:build !unix

package local

import (
	"fmt"
	"io/fs"
	"os"
	"runtime"
)

type platformTargetMetadata struct {
	linkCount uint64
}

func inspectPlatformTargetMetadata(fs.FileInfo) (platformTargetMetadata, error) {
	return platformTargetMetadata{}, fmt.Errorf("strict atomic overwrite metadata preservation is unsupported on %s", runtime.GOOS)
}

func applyPlatformTargetMetadata(*os.File, platformTargetMetadata) error { return nil }

func platformTargetMetadataSupported() bool { return false }
