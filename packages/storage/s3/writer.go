package s3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	tmtypes "github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager/types"
	"github.com/islishude/gotgz/packages/archiveutil"
	"github.com/islishude/gotgz/packages/locator"
)

// OpenWriter opens one streaming writer that uploads bytes into a single S3
// object when the caller closes the stream.
func (s *Store) OpenWriter(ctx context.Context, ref locator.Ref, metadata map[string]string) (io.WriteCloser, error) {
	return s.BeginWriter(ctx, ref, metadata)
}

// WriteSession is a streaming S3 upload that can be committed or aborted.
type WriteSession interface {
	io.WriteCloser
	Commit() error
	Abort(error) error
}

// BeginWriter starts a streaming upload whose multipart work can be canceled
// without completing the destination object.
func (s *Store) BeginWriter(ctx context.Context, ref locator.Ref, metadata map[string]string) (WriteSession, error) {
	if ref.Kind != locator.KindS3 {
		return nil, fmt.Errorf("ref %q is not s3", ref.Raw)
	}
	uploadCtx, cancel := context.WithCancel(ctx)
	pr, pw := io.Pipe()
	errCh := make(chan error, 1)
	in := &transfermanager.UploadObjectInput{
		Bucket:   new(ref.Bucket),
		Key:      new(ref.Key),
		Body:     pr,
		Metadata: archiveutil.MergeMetadata(ref.Metadata, metadata),
	}
	if contentType := archiveutil.ContentTypeForKey(ref.Key); contentType != "" {
		in.ContentType = new(contentType)
	}
	if cacheControl := strings.TrimSpace(ref.CacheControl); cacheControl != "" {
		in.CacheControl = new(cacheControl)
	}
	if tagging := encodeObjectTagging(ref.ObjectTags); tagging != "" {
		in.Tagging = new(tagging)
	}
	if err := s.applyEncryption(in); err != nil {
		cancel()
		_ = pr.Close()
		_ = pw.Close()
		return nil, err
	}
	go func() {
		_, err := s.tm.UploadObject(uploadCtx, in)
		// CloseWithError propagates the real upload failure through the pipe
		// so that ongoing pw.Write calls see the original error instead of the
		// generic io.ErrClosedPipe that bare pr.Close() would produce.
		if err != nil {
			_ = pr.CloseWithError(err)
		} else {
			_ = pr.Close()
		}
		errCh <- err
		close(errCh)
	}()
	return &uploadWriter{pw: pw, errCh: errCh, cancel: cancel}, nil
}

// UploadStream uploads one reader into a single S3 object without exposing an
// intermediate writer to the caller.
func (s *Store) UploadStream(ctx context.Context, ref locator.Ref, body io.Reader, metadata map[string]string) error {
	if ref.Kind != locator.KindS3 {
		return fmt.Errorf("ref %q is not s3", ref.Raw)
	}
	in := &transfermanager.UploadObjectInput{
		Bucket:   new(ref.Bucket),
		Key:      new(ref.Key),
		Body:     body,
		Metadata: archiveutil.MergeMetadata(ref.Metadata, metadata),
	}
	if contentType := archiveutil.ContentTypeForKey(ref.Key); contentType != "" {
		in.ContentType = new(contentType)
	}
	if cacheControl := strings.TrimSpace(ref.CacheControl); cacheControl != "" {
		in.CacheControl = new(cacheControl)
	}
	if tagging := encodeObjectTagging(ref.ObjectTags); tagging != "" {
		in.Tagging = new(tagging)
	}
	if err := s.applyEncryption(in); err != nil {
		return err
	}
	_, err := s.tm.UploadObject(ctx, in)
	return err
}

// applyEncryption maps gotgz encryption settings onto transfer-manager upload
// fields for every write path.
func (s *Store) applyEncryption(in *transfermanager.UploadObjectInput) error {
	if s.settings.SSEKMSKeyID != "" && s.settings.SSE != "aws:kms" && s.settings.SSE != "sse-kms" {
		return fmt.Errorf("SSE KMS key requires aws:kms or sse-kms encryption")
	}
	switch s.settings.SSE {
	case "", "aes256", "sse-s3":
		in.ServerSideEncryption = tmtypes.ServerSideEncryptionAes256
	case "aws:kms", "sse-kms":
		in.ServerSideEncryption = tmtypes.ServerSideEncryptionAwsKms
		if s.settings.SSEKMSKeyID != "" {
			in.SSEKMSKeyID = new(s.settings.SSEKMSKeyID)
		}
	case "none":
		return nil
	default:
		return fmt.Errorf("unsupported S3 server-side encryption %q", s.settings.SSE)
	}
	return nil
}

// encodeObjectTagging builds the S3 object tagging header string for one upload.
func encodeObjectTagging(tags map[string]string) string {
	values := make(url.Values, len(tags))
	for key, value := range tags {
		trimmedKey := strings.TrimSpace(key)
		if trimmedKey == "" {
			continue
		}
		values.Set(trimmedKey, strings.TrimSpace(value))
	}
	return values.Encode()
}

// uploadWriter bridges a streaming io.Writer caller to the asynchronous
// transfer-manager upload goroutine.
type uploadWriter struct {
	pw     *io.PipeWriter
	errCh  <-chan error
	cancel context.CancelFunc

	once sync.Once
	err  error
}

// Write forwards bytes into the upload pipe.
func (w *uploadWriter) Write(p []byte) (int, error) {
	return w.pw.Write(p)
}

// Close finalizes the upload stream and waits for the background upload result.
func (w *uploadWriter) Close() error {
	return w.Commit()
}

// Commit closes the upload body and waits for S3 to publish the object.
func (w *uploadWriter) Commit() error {
	w.once.Do(func() {
		closeErr := w.pw.Close()
		uploadErr := <-w.errCh
		if w.cancel != nil {
			w.cancel()
		}
		w.err = errors.Join(closeErr, uploadErr)
	})
	return w.err
}

// Abort cancels the upload and waits for multipart cleanup to finish.
func (w *uploadWriter) Abort(cause error) error {
	w.once.Do(func() {
		closeErr := w.pw.CloseWithError(cause)
		if w.cancel != nil {
			w.cancel()
		}
		uploadErr := <-w.errCh
		if errors.Is(uploadErr, cause) || errors.Is(uploadErr, context.Canceled) {
			uploadErr = nil
		}
		w.err = errors.Join(closeErr, uploadErr)
	})
	return w.err
}
