package s3

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	tmtypes "github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager/types"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/islishude/gotgz/packages/locator"
)

// OpenReader opens a full S3 object reader and uses transfer manager range
// downloads so large objects can be fetched concurrently.
func (s *Store) OpenReader(ctx context.Context, ref locator.Ref) (io.ReadCloser, Metadata, error) {
	if ref.Kind != locator.KindS3 {
		return nil, Metadata{}, fmt.Errorf("ref %q is not s3", ref.Raw)
	}
	readCtx, cancel := context.WithCancel(ctx)
	out, err := s.tm.GetObject(readCtx, &transfermanager.GetObjectInput{
		Bucket: new(ref.Bucket),
		Key:    new(ref.Key),
	}, func(o *transfermanager.Options) {
		o.GetObjectType = tmtypes.GetObjectRanges
	})
	if err != nil {
		cancel()
		return nil, Metadata{}, err
	}
	meta := Metadata{
		Size:        aws.ToInt64(out.ContentLength),
		ContentType: aws.ToString(out.ContentType),
	}
	return newDownloadReadCloser(out.Body, cancel), meta, nil
}

// OpenRangeReader opens one explicit byte range from an S3 object.
func (s *Store) OpenRangeReader(ctx context.Context, ref locator.Ref, offset int64, length int64) (io.ReadCloser, error) {
	if ref.Kind != locator.KindS3 {
		return nil, fmt.Errorf("ref %q is not s3", ref.Raw)
	}
	if offset < 0 {
		return nil, fmt.Errorf("range offset must be >= 0")
	}
	if length < 0 {
		return nil, fmt.Errorf("range length must be >= 0")
	}
	if length == 0 {
		return io.NopCloser(bytes.NewReader(nil)), nil
	}
	if offset > math.MaxInt64-(length-1) {
		return nil, fmt.Errorf("range end overflows int64 for offset %d and length %d", offset, length)
	}

	end := offset + length - 1
	rangeHeader := fmt.Sprintf("bytes=%d-%d", offset, end)
	out, err := s.client.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: new(ref.Bucket),
		Key:    new(ref.Key),
		Range:  &rangeHeader,
	})
	if err != nil {
		return nil, err
	}
	return out.Body, nil
}

// Stat returns S3 object metadata without opening the body stream.
func (s *Store) Stat(ctx context.Context, ref locator.Ref) (Metadata, error) {
	if ref.Kind != locator.KindS3 {
		return Metadata{}, fmt.Errorf("ref %q is not s3", ref.Raw)
	}
	out, err := s.client.HeadObject(ctx, &awss3.HeadObjectInput{Bucket: new(ref.Bucket), Key: new(ref.Key)})
	if err != nil {
		return Metadata{}, err
	}
	return Metadata{
		Size:        aws.ToInt64(out.ContentLength),
		ContentType: aws.ToString(out.ContentType),
	}, nil
}

// downloadReadCloser adapts transfer-manager readers to io.ReadCloser while
// preserving the ability to cancel in-flight multipart downloads on Close.
type downloadReadCloser struct {
	reader io.Reader
	close  func() error

	readMu  sync.Mutex
	readErr error

	once sync.Once
	err  error
}

// newDownloadReadCloser returns a reader that cancels the request context on
// Close and also closes the underlying reader when it exposes io.Closer.
func newDownloadReadCloser(reader io.Reader, cancel context.CancelFunc) io.ReadCloser {
	closer, _ := reader.(io.Closer)
	return &downloadReadCloser{
		reader: reader,
		close: func() error {
			if cancel != nil {
				cancel()
			}
			if closer != nil {
				return closer.Close()
			}
			return nil
		},
	}
}

// Read forwards reads to the wrapped transfer-manager body while constraining
// the forwarded slice capacity to its length. This avoids an upstream
// transfer-manager panic when callers pass a buffer with spare capacity and
// also memoizes terminal read errors so callers do not re-enter the underlying
// reader after it has failed.
func (r *downloadReadCloser) Read(p []byte) (int, error) {
	r.readMu.Lock()
	defer r.readMu.Unlock()

	if r.readErr != nil {
		return 0, r.readErr
	}

	n, err := r.reader.Read(p[:len(p):len(p)])
	if err != nil {
		r.readErr = err
	}
	return n, err
}

// Close cancels any in-flight download work and closes the wrapped reader once.
func (r *downloadReadCloser) Close() error {
	r.once.Do(func() {
		if r.close != nil {
			r.err = r.close()
		}
	})
	return r.err
}
