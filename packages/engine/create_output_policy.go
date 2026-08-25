package engine

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/islishude/gotgz/packages/archivepath"
	"github.com/islishude/gotgz/packages/cli"
	"github.com/islishude/gotgz/packages/locator"
)

// createOutputPolicy prevents a create operation from consuming its own
// destination or pre-existing volumes from the same output group.
type createOutputPolicy struct {
	localPath  string
	localInfo  fs.FileInfo
	localSplit *archivepath.SplitInfo

	s3Bucket string
	s3Key    string
	s3Split  *archivepath.SplitInfo

	ephemeralMu    sync.RWMutex
	ephemeralPaths map[string]fs.FileInfo
	ephemeralInfos []fs.FileInfo

	skipped atomic.Bool
}

func newCreateOutputPolicy(opts cli.Options) (*createOutputPolicy, error) {
	archiveName := opts.ResolvedArchive
	if archiveName == "" {
		archiveName = opts.Archive
	}
	if archiveName == "" || archiveName == "-" {
		return nil, nil
	}
	ref, err := locator.ParseArchive(archiveName)
	if err != nil {
		return nil, err
	}

	policy := &createOutputPolicy{}
	switch ref.Kind {
	case locator.KindLocal:
		absolute, err := filepath.Abs(filepath.Clean(ref.Path))
		if err != nil {
			return nil, err
		}
		policy.localPath = absolute
		if info, statErr := os.Lstat(absolute); statErr == nil {
			policy.localInfo = info
		} else if !os.IsNotExist(statErr) {
			return nil, statErr
		}
		if opts.SplitSizeBytes > 0 {
			first := archivepath.FormatSplit(absolute, 1, 4)
			if split, ok := archivepath.ParseSplit(first); ok {
				policy.localSplit = &split
			}
		}
	case locator.KindS3:
		policy.s3Bucket = ref.Bucket
		policy.s3Key = ref.Key
		if opts.SplitSizeBytes > 0 {
			first := archivepath.FormatSplit(ref.Key, 1, 4)
			if split, ok := archivepath.ParseSplit(first); ok {
				policy.s3Split = &split
			}
		}
	default:
		return nil, nil
	}
	return policy, nil
}

func (p *createOutputPolicy) rejectExplicitMember(ref locator.Ref, member, chdir string) error {
	if p == nil {
		return nil
	}
	switch ref.Kind {
	case locator.KindLocal:
		candidate := member
		if chdir != "" {
			candidate = filepath.Join(chdir, member)
		}
		absolute, err := filepath.Abs(filepath.Clean(candidate))
		if err != nil {
			return err
		}
		if p.matchesLocalPath(absolute) {
			return fmt.Errorf("archive output %q cannot also be an input member", absolute)
		}
		if p.localInfo != nil {
			if info, statErr := os.Lstat(absolute); statErr == nil && os.SameFile(p.localInfo, info) {
				return fmt.Errorf("archive output %q cannot also be an input member", absolute)
			}
		}
	case locator.KindS3:
		if p.matchesS3Ref(ref) {
			return fmt.Errorf("archive output s3://%s/%s cannot also be an input member", ref.Bucket, ref.Key)
		}
	}
	return nil
}

func (p *createOutputPolicy) shouldSkipLocal(path string) bool {
	if p == nil {
		return false
	}
	absolute, err := filepath.Abs(filepath.Clean(path))
	if err != nil {
		return false
	}
	p.ephemeralMu.RLock()
	_, ephemeral := p.ephemeralPaths[absolute]
	p.ephemeralMu.RUnlock()
	if ephemeral {
		return true
	}
	if p.localPath == "" || !p.matchesLocalPath(absolute) {
		return false
	}
	p.skipped.Store(true)
	return true
}

func (p *createOutputPolicy) shouldSkipEphemeralIdentity(info fs.FileInfo) bool {
	if p == nil || info == nil {
		return false
	}
	p.ephemeralMu.RLock()
	defer p.ephemeralMu.RUnlock()
	for _, artifact := range p.ephemeralInfos {
		if os.SameFile(artifact, info) {
			return true
		}
	}
	return false
}

// registerEphemeralLocalPaths records exact writer artifacts before recursive
// scanning starts. Registration fails closed if any artifact cannot be
// identified reliably.
func (p *createOutputPolicy) registerEphemeralLocalPaths(paths []string) error {
	if p == nil {
		return fmt.Errorf("create output policy is unavailable")
	}
	if len(paths) == 0 {
		return fmt.Errorf("transactional archive writer exposed no temporary output paths")
	}

	type artifact struct {
		path string
		info fs.FileInfo
	}
	artifacts := make([]artifact, 0, len(paths))
	for _, path := range paths {
		absolute, err := filepath.Abs(filepath.Clean(path))
		if err != nil {
			return fmt.Errorf("resolve temporary archive output %q: %w", path, err)
		}
		info, err := os.Lstat(absolute)
		if err != nil {
			return fmt.Errorf("identify temporary archive output %q: %w", absolute, err)
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("temporary archive output %q must be a regular file", absolute)
		}
		artifacts = append(artifacts, artifact{path: absolute, info: info})
	}

	p.ephemeralMu.Lock()
	defer p.ephemeralMu.Unlock()
	if p.ephemeralPaths == nil {
		p.ephemeralPaths = make(map[string]fs.FileInfo, len(artifacts))
	}
	for _, artifact := range artifacts {
		if _, exists := p.ephemeralPaths[artifact.path]; exists {
			continue
		}
		p.ephemeralPaths[artifact.path] = artifact.info
		p.ephemeralInfos = append(p.ephemeralInfos, artifact.info)
	}
	return nil
}

func (p *createOutputPolicy) matchesLocalPath(path string) bool {
	if p == nil || p.localPath == "" {
		return false
	}
	path = filepath.Clean(path)
	return p.matchesFinalLocalPath(path)
}

func (p *createOutputPolicy) matchesFinalLocalPath(path string) bool {
	if path == p.localPath {
		return true
	}
	if p.localSplit != nil {
		_, ok := archivepath.MatchSplit(path, *p.localSplit)
		return ok
	}
	return false
}

func (p *createOutputPolicy) matchesS3Ref(ref locator.Ref) bool {
	if p == nil || p.s3Bucket == "" || ref.Bucket != p.s3Bucket {
		return false
	}
	if ref.Key == p.s3Key {
		return true
	}
	if p.s3Split != nil {
		_, ok := archivepath.MatchSplit(ref.Key, *p.s3Split)
		return ok
	}
	return false
}

func (p *createOutputPolicy) outputWasSkipped() bool {
	return p != nil && p.skipped.Load()
}
