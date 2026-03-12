package engine

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/islishude/gotgz/packages/archivepath"
	"github.com/islishude/gotgz/packages/archiveutil"
	"github.com/islishude/gotgz/packages/locator"
)

// archiveVolume describes one readable archive file/object in a split sequence.
type archiveVolume struct {
	ref  locator.Ref
	info archiveReaderInfo
}

// resolveArchiveVolumes discovers all sibling parts when the input uses `.part0001`.
func (r *Runner) resolveArchiveVolumes(ctx context.Context, ref locator.Ref, firstInfo archiveReaderInfo) ([]archiveVolume, error) {
	split, ok := archivepath.ParseSplit(archiveutil.NameHint(ref))
	if !ok {
		return []archiveVolume{{ref: ref, info: firstInfo}}, nil
	}
	if split.Part != 1 {
		return nil, fmt.Errorf("split archives must be opened with part0001, got %s", archiveutil.NameHint(ref))
	}

	switch ref.Kind {
	case locator.KindLocal:
		return r.resolveLocalArchiveVolumes(ref, split, firstInfo)
	case locator.KindS3:
		return r.resolveS3ArchiveVolumes(ctx, ref, split, firstInfo)
	case locator.KindHTTP:
		return nil, fmt.Errorf("http(s) split archives are not supported")
	default:
		return []archiveVolume{{ref: ref, info: firstInfo}}, nil
	}
}

// resolveLocalArchiveVolumes lists matching files beside the first local volume.
func (r *Runner) resolveLocalArchiveVolumes(ref locator.Ref, split archivepath.SplitInfo, firstInfo archiveReaderInfo) ([]archiveVolume, error) {
	cleanPath := filepath.Clean(ref.Path)
	if normalizedSplit, ok := archivepath.ParseSplit(cleanPath); ok {
		split = normalizedSplit
	}

	dir := filepath.Dir(cleanPath)
	if dir == "." && !filepath.IsAbs(cleanPath) {
		dir = ""
	}
	listDir := dir
	if listDir == "" {
		listDir = "."
	}

	entries, err := os.ReadDir(listDir)
	if err != nil {
		return nil, err
	}

	found := make(map[int]archiveVolume)
	found[1] = archiveVolume{ref: ref, info: firstInfo}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		fullPath := entry.Name()
		if dir != "" {
			fullPath = filepath.Join(dir, entry.Name())
		}
		match, ok := archivepath.MatchSplit(fullPath, split)
		if !ok || match.Part == 1 {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return nil, err
		}
		found[match.Part] = archiveVolume{
			ref: locator.Ref{Kind: locator.KindLocal, Raw: fullPath, Path: fullPath},
			info: archiveReaderInfo{
				Size:      info.Size(),
				SizeKnown: true,
			},
		}
	}
	return sortedArchiveVolumes(found, split)
}

// resolveS3ArchiveVolumes lists matching objects under the first volume's prefix.
func (r *Runner) resolveS3ArchiveVolumes(ctx context.Context, ref locator.Ref, split archivepath.SplitInfo, firstInfo archiveReaderInfo) ([]archiveVolume, error) {
	prefix := split.DirPrefix + split.Stem + ".part"
	objects, err := r.storage.listS3Prefix(ctx, ref.Bucket, prefix)
	if err != nil {
		return nil, err
	}

	found := make(map[int]archiveVolume)
	found[1] = archiveVolume{ref: ref, info: firstInfo}
	for _, object := range objects {
		match, ok := archivepath.MatchSplit(object.Key, split)
		if !ok || match.Part == 1 {
			continue
		}
		found[match.Part] = archiveVolume{
			ref: locator.Ref{
				Kind:   locator.KindS3,
				Raw:    fmt.Sprintf("s3://%s/%s", ref.Bucket, object.Key),
				Bucket: ref.Bucket,
				Key:    object.Key,
			},
			info: archiveReaderInfo{
				Size:      object.Size,
				SizeKnown: true,
			},
		}
	}
	return sortedArchiveVolumes(found, split)
}

// sortedArchiveVolumes orders the discovered volumes and rejects gaps.
func sortedArchiveVolumes(found map[int]archiveVolume, split archivepath.SplitInfo) ([]archiveVolume, error) {
	parts := make([]int, 0, len(found))
	for part := range found {
		parts = append(parts, part)
	}
	sort.Ints(parts)
	width := max(split.Width, 4)
	for index, part := range parts {
		want := index + 1
		if part != want {
			missing := split.DirPrefix + fmt.Sprintf("%s.part%0*d%s", split.Stem, width, want, split.Suffix)
			return nil, fmt.Errorf("missing split archive volume %s", missing)
		}
	}

	volumes := make([]archiveVolume, 0, len(parts))
	for _, part := range parts {
		volumes = append(volumes, found[part])
	}
	return volumes, nil
}

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
		total += volume.info.Size
	}
	return total, true
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
