package s3

import (
	"context"
	"fmt"

	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

var _ s3APIClient = (*fakeTransferS3Client)(nil)

// fakeTransferS3Client implements the S3 operations used by Store and the
// built-in transfer manager with overridable test hooks.
type fakeTransferS3Client struct {
	putObjectFn               func(context.Context, *awss3.PutObjectInput, ...func(*awss3.Options)) (*awss3.PutObjectOutput, error)
	createMultipartUploadFn   func(context.Context, *awss3.CreateMultipartUploadInput, ...func(*awss3.Options)) (*awss3.CreateMultipartUploadOutput, error)
	uploadPartFn              func(context.Context, *awss3.UploadPartInput, ...func(*awss3.Options)) (*awss3.UploadPartOutput, error)
	completeMultipartUploadFn func(context.Context, *awss3.CompleteMultipartUploadInput, ...func(*awss3.Options)) (*awss3.CompleteMultipartUploadOutput, error)
	abortMultipartUploadFn    func(context.Context, *awss3.AbortMultipartUploadInput, ...func(*awss3.Options)) (*awss3.AbortMultipartUploadOutput, error)
	headObjectFn              func(context.Context, *awss3.HeadObjectInput, ...func(*awss3.Options)) (*awss3.HeadObjectOutput, error)
	getObjectFn               func(context.Context, *awss3.GetObjectInput, ...func(*awss3.Options)) (*awss3.GetObjectOutput, error)
	listObjectsV2Fn           func(context.Context, *awss3.ListObjectsV2Input, ...func(*awss3.Options)) (*awss3.ListObjectsV2Output, error)
}

func (c *fakeTransferS3Client) PutObject(ctx context.Context, in *awss3.PutObjectInput, optFns ...func(*awss3.Options)) (*awss3.PutObjectOutput, error) {
	if c.putObjectFn == nil {
		return nil, fmt.Errorf("unexpected PutObject call")
	}
	return c.putObjectFn(ctx, in, optFns...)
}

func (c *fakeTransferS3Client) UploadPart(ctx context.Context, in *awss3.UploadPartInput, optFns ...func(*awss3.Options)) (*awss3.UploadPartOutput, error) {
	if c.uploadPartFn == nil {
		return nil, fmt.Errorf("unexpected UploadPart call")
	}
	return c.uploadPartFn(ctx, in, optFns...)
}

func (c *fakeTransferS3Client) CreateMultipartUpload(ctx context.Context, in *awss3.CreateMultipartUploadInput, optFns ...func(*awss3.Options)) (*awss3.CreateMultipartUploadOutput, error) {
	if c.createMultipartUploadFn == nil {
		return nil, fmt.Errorf("unexpected CreateMultipartUpload call")
	}
	return c.createMultipartUploadFn(ctx, in, optFns...)
}

func (c *fakeTransferS3Client) CompleteMultipartUpload(ctx context.Context, in *awss3.CompleteMultipartUploadInput, optFns ...func(*awss3.Options)) (*awss3.CompleteMultipartUploadOutput, error) {
	if c.completeMultipartUploadFn == nil {
		return nil, fmt.Errorf("unexpected CompleteMultipartUpload call")
	}
	return c.completeMultipartUploadFn(ctx, in, optFns...)
}

func (c *fakeTransferS3Client) AbortMultipartUpload(ctx context.Context, in *awss3.AbortMultipartUploadInput, optFns ...func(*awss3.Options)) (*awss3.AbortMultipartUploadOutput, error) {
	if c.abortMultipartUploadFn == nil {
		return nil, fmt.Errorf("unexpected AbortMultipartUpload call")
	}
	return c.abortMultipartUploadFn(ctx, in, optFns...)
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
func (c *fakeTransferS3Client) ListObjectsV2(ctx context.Context, in *awss3.ListObjectsV2Input, optFns ...func(*awss3.Options)) (*awss3.ListObjectsV2Output, error) {
	if c.listObjectsV2Fn == nil {
		return nil, fmt.Errorf("unexpected ListObjectsV2 call")
	}
	return c.listObjectsV2Fn(ctx, in, optFns...)
}

// expectedByteRanges returns the concurrent range requests expected for a
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

func newTestTransferManager(client transferAPIClient, partSize int64, concurrency int) *transferManager {
	return newTransferManager(client, transferOptions{
		partSize:           partSize,
		multipartThreshold: 8,
		concurrency:        concurrency,
		bodyAttempts:       defaultTransferAttempts,
	})
}
