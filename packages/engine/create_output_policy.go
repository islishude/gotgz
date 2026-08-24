package engine

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
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
	if p == nil || p.localPath == "" {
		return false
	}
	absolute, err := filepath.Abs(filepath.Clean(path))
	if err != nil || !p.matchesLocalPath(absolute) {
		return false
	}
	p.skipped.Store(true)
	return true
}

func (p *createOutputPolicy) matchesLocalPath(path string) bool {
	if p == nil || p.localPath == "" {
		return false
	}
	path = filepath.Clean(path)
	if p.matchesFinalLocalPath(path) {
		return true
	}
	base := filepath.Base(path)
	if strings.HasPrefix(base, ".") {
		if marker := strings.LastIndex(base, ".gotgz-"); marker > 1 {
			stagedTarget := filepath.Join(filepath.Dir(path), base[1:marker])
			return p.matchesFinalLocalPath(stagedTarget)
		}
	}
	return false
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
