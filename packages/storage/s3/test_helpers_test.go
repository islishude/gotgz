package s3

import (
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

var _ transfermanager.S3APIClient = (*fakeTransferS3Client)(nil)

// fakeTransferS3Client implements the transfer-manager S3 interface with
// overridable hooks for the download-focused tests in this package.
type fakeTransferS3Client struct {
	headObjectFn func(context.Context, *awss3.HeadObjectInput, ...func(*awss3.Options)) (*awss3.HeadObjectOutput, error)
	getObjectFn  func(context.Context, *awss3.GetObjectInput, ...func(*awss3.Options)) (*awss3.GetObjectOutput, error)
}

// PutObject rejects unexpected upload calls in download tests.
func (c *fakeTransferS3Client) PutObject(context.Context, *awss3.PutObjectInput, ...func(*awss3.Options)) (*awss3.PutObjectOutput, error) {
	return nil, fmt.Errorf("unexpected PutObject call")
}

// UploadPart rejects unexpected multipart-upload calls in download tests.
func (c *fakeTransferS3Client) UploadPart(context.Context, *awss3.UploadPartInput, ...func(*awss3.Options)) (*awss3.UploadPartOutput, error) {
	return nil, fmt.Errorf("unexpected UploadPart call")
}

// CreateMultipartUpload rejects unexpected multipart-upload calls in download tests.
func (c *fakeTransferS3Client) CreateMultipartUpload(context.Context, *awss3.CreateMultipartUploadInput, ...func(*awss3.Options)) (*awss3.CreateMultipartUploadOutput, error) {
	return nil, fmt.Errorf("unexpected CreateMultipartUpload call")
}

// CompleteMultipartUpload rejects unexpected multipart-upload calls in download tests.
func (c *fakeTransferS3Client) CompleteMultipartUpload(context.Context, *awss3.CompleteMultipartUploadInput, ...func(*awss3.Options)) (*awss3.CompleteMultipartUploadOutput, error) {
	return nil, fmt.Errorf("unexpected CompleteMultipartUpload call")
}

// AbortMultipartUpload rejects unexpected multipart-upload calls in download tests.
func (c *fakeTransferS3Client) AbortMultipartUpload(context.Context, *awss3.AbortMultipartUploadInput, ...func(*awss3.Options)) (*awss3.AbortMultipartUploadOutput, error) {
	return nil, fmt.Errorf("unexpected AbortMultipartUpload call")
}

// GetObject delegates to the test hook when provided.
func (c *fakeTransferS3Client) GetObject(ctx context.Context, in *awss3.GetObjectInput, optFns ...func(*awss3.Options)) (*awss3.GetObjectOutput, error) {
	if c.getObjectFn == nil {
		return nil, fmt.Errorf("unexpected GetObject call")
	}
	return c.getObjectFn(ctx, in, optFns...)
}

// HeadObject delegates to the test hook when provided.
func (c *fakeTransferS3Client) HeadObject(ctx context.Context, in *awss3.HeadObjectInput, optFns ...func(*awss3.Options)) (*awss3.HeadObjectOutput, error) {
	if c.headObjectFn == nil {
		return nil, fmt.Errorf("unexpected HeadObject call")
	}
	return c.headObjectFn(ctx, in, optFns...)
}

// ListObjectsV2 rejects unexpected listing calls in download tests.
func (c *fakeTransferS3Client) ListObjectsV2(context.Context, *awss3.ListObjectsV2Input, ...func(*awss3.Options)) (*awss3.ListObjectsV2Output, error) {
	return nil, fmt.Errorf("unexpected ListObjectsV2 call")
}

// closeTrackingReader records Close calls so wrapper tests can verify
// idempotent cleanup behavior.
type closeTrackingReader struct {
	reader     io.Reader
	closeErr   error
	closeCalls int
}

// Read forwards reads to the wrapped reader.
func (r *closeTrackingReader) Read(p []byte) (int, error) {
	return r.reader.Read(p)
}

// Close records the call and returns the configured error.
func (r *closeTrackingReader) Close() error {
	r.closeCalls++
	return r.closeErr
}

// expectedByteRanges returns the transfer-manager range requests expected for a
// given payload length and part size.
func expectedByteRanges(total int, partSize int64) []string {
	ranges := make([]string, 0)
	for start := int64(0); start < int64(total); start += partSize {
		end := start + partSize - 1
		if end >= int64(total) {
			end = int64(total) - 1
		}
		ranges = append(ranges, fmt.Sprintf("bytes=%d-%d", start, end))
	}
	return ranges
}
