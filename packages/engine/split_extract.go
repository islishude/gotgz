package engine

import (
	"archive/tar"
	"archive/zip"
	"context"
	"io"
	"math"
	"path"
	"sort"
	"strings"
	"sync"

	"github.com/islishude/gotgz/packages/archivepath"
	"github.com/islishude/gotgz/packages/archiveutil"
	"github.com/islishude/gotgz/packages/cli"
	"github.com/islishude/gotgz/packages/locator"
)

const maxSplitExtractConcurrency = 4

type splitExtractSerialReason string

const (
	splitExtractSerialReasonNone           splitExtractSerialReason = ""
	splitExtractSerialReasonLocalDuplicate splitExtractSerialReason = "local-duplicate-path"
	splitExtractSerialReasonLocalAncestor  splitExtractSerialReason = "local-nondir-ancestor"
	splitExtractSerialReasonLocalHardlink  splitExtractSerialReason = "local-hardlink-dependency"
	splitExtractSerialReasonS3Duplicate    splitExtractSerialReason = "s3-duplicate-key"
)

// splitExtractPlan stores the planner decision for one split extract run.
type splitExtractPlan struct {
	parallel        bool
	serialReason    splitExtractSerialReason
	zipPayloadBytes int64
}

// splitExtractManifestEntry stores one extracted output candidate discovered during planning.
type splitExtractManifestEntry struct {
	volumeIndex    int
	outputPath     string
	isDir          bool
	hardlinkTarget string
}

// splitExtractPlanner accumulates manifest entries and zip payload totals for one split extract scan.
type splitExtractPlanner struct {
	target          locator.Ref
	opts            cli.Options
	memberMatcher   *archivepath.CompiledPathMatcher
	manifest        []splitExtractManifestEntry
	zipPayloadBytes int64
}

// newSplitExtractPlanner creates one planner for a split extract scan.
func newSplitExtractPlanner(opts cli.Options, target locator.Ref, memberMatcher *archivepath.CompiledPathMatcher) *splitExtractPlanner {
	return &splitExtractPlanner{
		target:        target,
		opts:          opts,
		memberMatcher: memberMatcher,
		manifest:      make([]splitExtractManifestEntry, 0),
	}
}

// shouldPlanSplitExtract reports whether multi-volume extract should run the safety planner.
func shouldPlanSplitExtract(opts cli.Options, volumes []archiveVolume) bool {
	return len(volumes) > 1 && !opts.ToStdout
}

// splitExtractWorkerCount bounds concurrent split extract workers.
func splitExtractWorkerCount(volumeCount int) int {
	if volumeCount <= 0 {
		return 0
	}
	if volumeCount < maxSplitExtractConcurrency {
		return volumeCount
	}
	return maxSplitExtractConcurrency
}

// planSplitTarExtract scans split tar volumes and decides whether concurrent extract is safe.
func (r *Runner) planSplitTarExtract(ctx context.Context, opts cli.Options, volumes []archiveVolume, first io.ReadCloser, firstInfo archiveReaderInfo, target locator.Ref) (splitExtractPlan, error) {
	planner := newSplitExtractPlanner(opts, target, archivepath.NewMemberMatcher(opts.Members, opts.Wildcards))
	volumeIndex := 0

	_, err := r.forEachArchiveVolume(ctx, volumes, first, firstInfo, func(ref locator.Ref, reader io.ReadCloser, readerInfo archiveReaderInfo) (int, error) {
		currentVolume := volumeIndex
		volumeIndex++

		_, err := r.scanTarArchiveFromReader(ctx, opts, nil, readerInfo, archiveutil.NameHint(ref), reader, func(hdr *tar.Header, _ *tar.Reader) (int, error) {
			planner.recordTarEntry(currentVolume, hdr)
			return 0, nil
		})
		return 0, err
	})
	if err != nil {
		return splitExtractPlan{}, err
	}

	plan := planner.finalize()
	r.notifySplitExtractPlan(plan)
	return plan, nil
}

// planSplitZipExtract scans split zip volumes and decides whether concurrent extract is safe.
func (r *Runner) planSplitZipExtract(ctx context.Context, opts cli.Options, volumes []archiveVolume, first io.ReadCloser, firstInfo archiveReaderInfo, target locator.Ref) (splitExtractPlan, error) {
	planner := newSplitExtractPlanner(opts, target, archivepath.NewMemberMatcher(opts.Members, opts.Wildcards))
	volumeIndex := 0

	_, err := r.forEachArchiveVolume(ctx, volumes, first, firstInfo, func(ref locator.Ref, reader io.ReadCloser, readerInfo archiveReaderInfo) (int, error) {
		currentVolume := volumeIndex
		volumeIndex++

		_, err := r.withZipReader(ctx, ref, reader, readerInfo, nil, func(zr *zip.Reader) (int, error) {
			for _, zf := range zr.File {
				planner.recordZipEntry(currentVolume, zf)
			}
			return 0, nil
		})
		return 0, err
	})
	if err != nil {
		return splitExtractPlan{}, err
	}

	plan := planner.finalize()
	r.notifySplitExtractPlan(plan)
	return plan, nil
}

// recordTarEntry records one tar member when it would affect extract output.
func (p *splitExtractPlanner) recordTarEntry(volumeIndex int, hdr *tar.Header) {
	if hdr == nil || archivepath.ShouldSkipMemberWithMatcher(p.memberMatcher, hdr.Name) {
		return
	}

	extractName, ok := archivepath.StripPathComponents(hdr.Name, p.opts.StripComponents)
	if !ok {
		return
	}

	switch p.target.Kind {
	case locator.KindS3:
		p.recordTarS3Entry(volumeIndex, hdr, extractName)
	default:
		p.recordTarLocalEntry(volumeIndex, hdr, extractName)
	}
}

// recordTarLocalEntry records one tar member that would mutate the local filesystem.
func (p *splitExtractPlanner) recordTarLocalEntry(volumeIndex int, hdr *tar.Header, extractName string) {
	switch hdr.Typeflag {
	case tar.TypeDir, tar.TypeReg, tar.TypeSymlink, tar.TypeLink:
	default:
		return
	}

	entry := splitExtractManifestEntry{
		volumeIndex: volumeIndex,
		outputPath:  cleanSplitExtractLocalPath(extractName),
		isDir:       hdr.Typeflag == tar.TypeDir,
	}
	if hdr.Typeflag == tar.TypeLink {
		entry.hardlinkTarget = cleanSplitExtractLocalPath(hdr.Linkname)
	}
	p.manifest = append(p.manifest, entry)
}

// recordTarS3Entry records one tar member that would upload an S3 object.
func (p *splitExtractPlanner) recordTarS3Entry(volumeIndex int, hdr *tar.Header, extractName string) {
	if hdr.Typeflag == tar.TypeDir {
		return
	}

	key, ok := splitExtractS3Key(p.target, extractName)
	if !ok {
		return
	}

	p.manifest = append(p.manifest, splitExtractManifestEntry{
		volumeIndex: volumeIndex,
		outputPath:  key,
	})
}

// recordZipEntry records one zip member when it contributes payload or output.
func (p *splitExtractPlanner) recordZipEntry(volumeIndex int, zf *zip.File) {
	if zf == nil {
		return
	}
	if shouldIncludeZipExtractEntry(zf, p.memberMatcher, p.opts.StripComponents) && !isZipDir(zf) {
		p.zipPayloadBytes = addArchiveVolumeSize(p.zipPayloadBytes, zipFilePayloadBytes(zf))
	}
	if archivepath.ShouldSkipMemberWithMatcher(p.memberMatcher, zf.Name) {
		return
	}

	extractName, ok := archivepath.StripPathComponents(zf.Name, p.opts.StripComponents)
	if !ok || extractName == "" {
		return
	}

	switch p.target.Kind {
	case locator.KindS3:
		p.recordZipS3Entry(volumeIndex, zf, extractName)
	default:
		p.recordZipLocalEntry(volumeIndex, zf, extractName)
	}
}

// recordZipLocalEntry records one zip member that would mutate the local filesystem.
func (p *splitExtractPlanner) recordZipLocalEntry(volumeIndex int, zf *zip.File, extractName string) {
	switch {
	case isZipDir(zf), isZipRegular(zf), isZipSymlink(zf):
	default:
		return
	}

	p.manifest = append(p.manifest, splitExtractManifestEntry{
		volumeIndex: volumeIndex,
		outputPath:  cleanSplitExtractLocalPath(extractName),
		isDir:       isZipDir(zf),
	})
}

// recordZipS3Entry records one zip member that would upload an S3 object.
func (p *splitExtractPlanner) recordZipS3Entry(volumeIndex int, zf *zip.File, extractName string) {
	if isZipDir(zf) || (!isZipRegular(zf) && !isZipSymlink(zf)) {
		return
	}

	key, ok := splitExtractS3Key(p.target, extractName)
	if !ok {
		return
	}

	p.manifest = append(p.manifest, splitExtractManifestEntry{
		volumeIndex: volumeIndex,
		outputPath:  key,
	})
}

// finalize evaluates the accumulated manifest and returns the final planner decision.
func (p *splitExtractPlanner) finalize() splitExtractPlan {
	plan := splitExtractPlan{
		parallel:        true,
		zipPayloadBytes: p.zipPayloadBytes,
	}

	switch p.target.Kind {
	case locator.KindS3:
		plan.serialReason = evaluateSplitExtractS3Manifest(p.manifest)
	default:
		plan.serialReason = evaluateSplitExtractLocalManifest(p.manifest)
	}
	plan.parallel = plan.serialReason == splitExtractSerialReasonNone
	return plan
}

// evaluateSplitExtractLocalManifest decides whether local output paths are safe for concurrent extract.
func evaluateSplitExtractLocalManifest(entries []splitExtractManifestEntry) splitExtractSerialReason {
	if hasCrossVolumeDuplicateNonDir(entries) {
		return splitExtractSerialReasonLocalDuplicate
	}
	if hasCrossVolumeNonDirAncestor(entries) {
		return splitExtractSerialReasonLocalAncestor
	}
	if hasCrossVolumeHardlinkDependency(entries) {
		return splitExtractSerialReasonLocalHardlink
	}
	return splitExtractSerialReasonNone
}

// evaluateSplitExtractS3Manifest decides whether S3 object keys are safe for concurrent extract.
func evaluateSplitExtractS3Manifest(entries []splitExtractManifestEntry) splitExtractSerialReason {
	if hasCrossVolumeDuplicateKey(entries) {
		return splitExtractSerialReasonS3Duplicate
	}
	return splitExtractSerialReasonNone
}

// hasCrossVolumeDuplicateNonDir reports whether one output path is repeated across volumes as a non-directory.
func hasCrossVolumeDuplicateNonDir(entries []splitExtractManifestEntry) bool {
	grouped := make(map[string][]splitExtractManifestEntry, len(entries))
	for _, entry := range entries {
		grouped[entry.outputPath] = append(grouped[entry.outputPath], entry)
	}

	for _, group := range grouped {
		if len(group) < 2 {
			continue
		}

		volumes := make(map[int]struct{}, len(group))
		allDirs := true
		for _, entry := range group {
			volumes[entry.volumeIndex] = struct{}{}
			allDirs = allDirs && entry.isDir
		}
		if len(volumes) > 1 && !allDirs {
			return true
		}
	}
	return false
}

// hasCrossVolumeNonDirAncestor reports whether a non-directory output path would be a cross-volume ancestor.
func hasCrossVolumeNonDirAncestor(entries []splitExtractManifestEntry) bool {
	sortedEntries := append([]splitExtractManifestEntry(nil), entries...)
	sort.Slice(sortedEntries, func(i, j int) bool {
		if sortedEntries[i].outputPath == sortedEntries[j].outputPath {
			return sortedEntries[i].volumeIndex < sortedEntries[j].volumeIndex
		}
		return sortedEntries[i].outputPath < sortedEntries[j].outputPath
	})

	for index, ancestor := range sortedEntries {
		if ancestor.isDir {
			continue
		}
		for next := index + 1; next < len(sortedEntries); next++ {
			descendant := sortedEntries[next]
			if descendant.outputPath == ancestor.outputPath {
				continue
			}
			if !isSplitExtractDescendant(descendant.outputPath, ancestor.outputPath) {
				break
			}
			if descendant.volumeIndex != ancestor.volumeIndex {
				return true
			}
		}
	}
	return false
}

// hasCrossVolumeHardlinkDependency reports whether a hardlink depends on another volume's extracted output.
func hasCrossVolumeHardlinkDependency(entries []splitExtractManifestEntry) bool {
	outputVolumes := make(map[string]map[int]struct{}, len(entries))
	for _, entry := range entries {
		if _, ok := outputVolumes[entry.outputPath]; !ok {
			outputVolumes[entry.outputPath] = make(map[int]struct{})
		}
		outputVolumes[entry.outputPath][entry.volumeIndex] = struct{}{}
	}

	for _, entry := range entries {
		if entry.hardlinkTarget == "" {
			continue
		}
		for volumeIndex := range outputVolumes[entry.hardlinkTarget] {
			if volumeIndex != entry.volumeIndex {
				return true
			}
		}
	}
	return false
}

// hasCrossVolumeDuplicateKey reports whether an S3 object key is produced by multiple volumes.
func hasCrossVolumeDuplicateKey(entries []splitExtractManifestEntry) bool {
	firstVolumeByKey := make(map[string]int, len(entries))
	for _, entry := range entries {
		if firstVolume, ok := firstVolumeByKey[entry.outputPath]; ok {
			if firstVolume != entry.volumeIndex {
				return true
			}
			continue
		}
		firstVolumeByKey[entry.outputPath] = entry.volumeIndex
	}
	return false
}

// isSplitExtractDescendant reports whether candidate is nested below ancestor.
func isSplitExtractDescendant(candidate string, ancestor string) bool {
	if ancestor == "" {
		return candidate != ""
	}
	return strings.HasPrefix(candidate, ancestor+"/")
}

// cleanSplitExtractLocalPath normalizes one extracted local path for planner comparisons.
func cleanSplitExtractLocalPath(name string) string {
	clean := path.Clean(strings.TrimPrefix(name, "/"))
	if clean == "." {
		return ""
	}
	return clean
}

// splitExtractS3Key resolves one extracted member name into its final S3 object key.
func splitExtractS3Key(target locator.Ref, extractName string) (string, bool) {
	name := strings.TrimPrefix(extractName, "./")
	if name == "" {
		return "", false
	}
	return locator.JoinS3Prefix(target.Key, name), true
}

// zipFilePayloadBytes converts one zip file's uncompressed size into a clamped int64.
func zipFilePayloadBytes(zf *zip.File) int64 {
	if zf.UncompressedSize64 > uint64(math.MaxInt64) {
		return math.MaxInt64
	}
	return int64(zf.UncompressedSize64)
}

type splitExtractVolumeTask struct {
	index  int
	volume archiveVolume
}

type splitExtractVolumeResult struct {
	warnings int
}

// executeSplitExtractVolumes runs one volume worker pool and cancels remaining work on the first error.
func (r *Runner) executeSplitExtractVolumes(ctx context.Context, volumes []archiveVolume, fn func(context.Context, int, archiveVolume) (int, error)) (int, error) {
	workerCount := splitExtractWorkerCount(len(volumes))
	if workerCount == 0 {
		return 0, nil
	}

	workCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	tasksCh := make(chan splitExtractVolumeTask)
	resultsCh := make(chan splitExtractVolumeResult, workerCount)

	var workers sync.WaitGroup
	for range workerCount {
		workers.Go(func() {
			for {
				select {
				case <-workCtx.Done():
					return
				case task, ok := <-tasksCh:
					if !ok {
						return
					}

					r.notifySplitExtractWorkerStart(task.index, task.volume.ref)
					warnings, err := fn(workCtx, task.index, task.volume)
					if err != nil {
						cancel(err)
						return
					}

					select {
					case resultsCh <- splitExtractVolumeResult{warnings: warnings}:
					case <-workCtx.Done():
						return
					}
				}
			}
		})
	}

	go func() {
		defer close(tasksCh)
		for index, volume := range volumes {
			select {
			case <-workCtx.Done():
				return
			case tasksCh <- splitExtractVolumeTask{index: index, volume: volume}:
			}
		}
	}()

	go func() {
		workers.Wait()
		close(resultsCh)
	}()

	warnings := 0
	for result := range resultsCh {
		warnings += result.warnings
	}
	if err := context.Cause(workCtx); err != nil {
		return warnings, err
	}
	return warnings, nil
}
