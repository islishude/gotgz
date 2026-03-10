package engine

import (
	"context"
	"io"
	"path/filepath"
	"time"

	"github.com/islishude/gotgz/internal/locator"
)

// streamS3MemberToArchive opens one S3 member, streams it into an archive writer callback,
// and preserves the existing verbose/progress behavior shared by tar and zip creation.
func (r *Runner) streamS3MemberToArchive(ctx context.Context, ref locator.Ref, verbose bool, reporter *progressReporter, write func(name string, size int64, modified time.Time, body io.Reader) error) (err error) {
	body, meta, err := r.storage.openS3ObjectReader(ctx, ref)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := body.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()

	name := filepath.ToSlash(ref.Key)
	if err := write(name, meta.Size, time.Now(), newCountingReader(body, reporter)); err != nil {
		return err
	}
	if verbose {
		reporter.beforeExternalLineOutput()
		_, _ = io.WriteString(r.stdout, name+"\n")
		reporter.afterExternalLineOutput()
	}
	return nil
}

// uploadToS3Target writes one extracted entry into an S3 target prefix.
func (r *Runner) uploadToS3Target(ctx context.Context, target locator.Ref, name string, body io.Reader, metadata map[string]string) error {
	return r.storage.uploadS3Object(ctx, locator.Ref{
		Kind:         locator.KindS3,
		Bucket:       target.Bucket,
		Key:          locator.JoinS3Prefix(target.Key, name),
		CacheControl: target.CacheControl,
	}, body, metadata)
}
