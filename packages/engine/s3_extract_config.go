package engine

import (
	"os"
	"path/filepath"

	s3store "github.com/islishude/gotgz/packages/storage/s3"
)

const (
	defaultS3ExtractWorkers      = 1
	defaultS3ExtractStagingBytes = 512 << 20
)

// s3ExtractConfig controls archive->S3 extraction concurrency and staging.
type s3ExtractConfig struct {
	workers      int
	stagingBytes int64
	stagingDir   string
}

// runnerOption customizes one Runner during construction.
type runnerOption func(*Runner)

// withS3ExtractConfig applies S3 extract concurrency settings to one Runner.
func withS3ExtractConfig(cfg s3ExtractConfig) runnerOption {
	return func(r *Runner) {
		r.s3Extract = cfg
	}
}

// defaultS3ExtractConfig returns conservative defaults used by unit-test helpers.
func defaultS3ExtractConfig() s3ExtractConfig {
	return s3ExtractConfig{
		workers:      defaultS3ExtractWorkers,
		stagingBytes: defaultS3ExtractStagingBytes,
		stagingDir:   filepath.Clean(os.TempDir()),
	}
}

// newS3ExtractConfigFromStoreSettings maps storage-level env config onto engine behavior.
func newS3ExtractConfigFromStoreSettings(settings s3store.Settings) s3ExtractConfig {
	cfg := defaultS3ExtractConfig()
	cfg.workers = settings.ExtractWorkers
	if cfg.workers < 1 {
		cfg.workers = defaultS3ExtractWorkers
	}
	if settings.ExtractStagingBytes > 0 {
		cfg.stagingBytes = settings.ExtractStagingBytes
	}
	if settings.ExtractStagingDir != "" {
		cfg.stagingDir = settings.ExtractStagingDir
	}
	return cfg
}
