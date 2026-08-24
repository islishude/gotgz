package s3

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/islishude/gotgz/packages/archiveutil"
	"github.com/islishude/gotgz/packages/locator"
)

// OpenReader opens a full S3 object reader and uses bounded concurrent range
// downloads so large objects can be fetched efficiently.
func (s *Store) OpenReader(ctx context.Context, ref locator.Ref) (io.ReadCloser, Metadata, error) {
	if ref.Kind != locator.KindS3 {
		return nil, Metadata{}, fmt.Errorf("ref %q is not s3", ref.Raw)
	}
	if s.transfers == nil {
		return nil, Metadata{}, fmt.Errorf("s3 transfer manager is not configured")
	}
	return s.transfers.openReader(ctx, ref.Bucket, ref.Key)
}

// OpenRangeReader opens one explicit byte range from an S3 object.
func (s *Store) OpenRangeReader(ctx context.Context, ref locator.Ref, offset int64, length int64) (io.ReadCloser, error) {
	return s.OpenRangeReaderSnapshot(ctx, ref, offset, length, archiveutil.Snapshot{})
}

// OpenRangeReaderSnapshot opens an exact byte range fenced to one S3 object
// version or ETag.
func (s *Store) OpenRangeReaderSnapshot(ctx context.Context, ref locator.Ref, offset int64, length int64, snapshot archiveutil.Snapshot) (io.ReadCloser, error) {
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
	input := &awss3.GetObjectInput{
		Bucket: new(ref.Bucket),
		Key:    new(ref.Key),
		Range:  &rangeHeader,
	}
	if snapshot.VersionID != "" {
		input.VersionId = new(snapshot.VersionID)
	} else if snapshot.ETag != "" {
		input.IfMatch = new(snapshot.ETag)
	}
	if s.client == nil {
		return nil, fmt.Errorf("s3 range client is not configured")
	}
	out, err := s.client.GetObject(ctx, input)
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
	if s.client == nil {
		return Metadata{}, fmt.Errorf("s3 client is not configured")
	}
	out, err := s.client.HeadObject(ctx, &awss3.HeadObjectInput{Bucket: new(ref.Bucket), Key: new(ref.Key)})
	if err != nil {
		return Metadata{}, err
	}
	return Metadata{
		Size:         aws.ToInt64(out.ContentLength),
		ContentType:  aws.ToString(out.ContentType),
		ETag:         aws.ToString(out.ETag),
		VersionID:    aws.ToString(out.VersionId),
		LastModified: aws.ToTime(out.LastModified),
	}, nil
}
