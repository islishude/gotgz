package engine

import (
	"context"
	"io"
	"math"

	"github.com/islishude/gotgz/packages/locator"
)

// mergeArchiveReaderInfo keeps discovered size info while filling runtime metadata from the store.
func mergeArchiveReaderInfo(base archiveReaderInfo, runtime archiveReaderInfo) archiveReaderInfo {
	out := base
	if runtime.SizeKnown {
		out.Size = runtime.Size
		out.SizeKnown = true
	}
	if runtime.ContentType != "" {
		out.ContentType = runtime.ContentType
	}
	return out
}

// sumArchiveVolumeSizes reports the combined size for a discovered split archive set.
func sumArchiveVolumeSizes(volumes []archiveVolume) (int64, bool) {
	var total int64
	for _, volume := range volumes {
		if !volume.info.SizeKnown {
			return 0, false
		}
		total = addArchiveVolumeSize(total, volume.info.Size)
	}
	return total, true
}

// addArchiveVolumeSize accumulates discovered volume sizes while clamping on overflow.
func addArchiveVolumeSize(total int64, size int64) int64 {
	if size <= 0 || total == math.MaxInt64 {
		return total
	}
	if math.MaxInt64-total < size {
		return math.MaxInt64
	}
	return total + size
}

// forEachArchiveVolume iterates split archive volumes in order and opens each reader on demand.
func (r *Runner) forEachArchiveVolume(ctx context.Context, volumes []archiveVolume, first io.ReadCloser, firstInfo archiveReaderInfo, fn func(ref locator.Ref, reader io.ReadCloser, info archiveReaderInfo) (int, error)) (int, error) {
	warnings := 0
	for index, volume := range volumes {
		select {
		case <-ctx.Done():
			return warnings, ctx.Err()
		default:
		}

		reader := first
		info := mergeArchiveReaderInfo(volume.info, firstInfo)
		openedHere := false
		if index > 0 || first == nil {
			var err error
			reader, info, err = r.openArchiveReader(ctx, volume.ref)
			if err != nil {
				return warnings, err
			}
			info = mergeArchiveReaderInfo(volume.info, info)
			openedHere = true
		}

		w, err := fn(volume.ref, reader, info)
		warnings += w
		if openedHere {
			_ = reader.Close()
		}
		if err != nil {
			return warnings, err
		}
	}
	return warnings, nil
}
