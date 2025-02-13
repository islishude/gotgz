package gotgz

import (
	"context"
	"errors"
	"io"
	"net/url"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	s3manager "github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type S3 struct {
	uploader *s3manager.Uploader
	s3Client *s3.Client
	bucket   string
}

func New(basectx context.Context, bucket string) (S3, error) {
	sdkConfig, err := config.LoadDefaultConfig(basectx)
	if err != nil {
		return S3{}, err
	}

	s3Client := s3.NewFromConfig(sdkConfig)
	return NewWithClient(s3Client, bucket), nil
}

func NewWithClient(s3c *s3.Client, bucket string) S3 {
	return S3{
		uploader: s3manager.NewUploader(s3c),
		s3Client: s3c,
		bucket:   bucket,
	}
}

func (s S3) Upload(ctx context.Context, flags CompressFlags, s3Key string, sources ...string) error {
	reader, writer := io.Pipe()

	errChan := make(chan error)
	go func() {
		errChan <- Compress(ctx, writer, flags, sources...)
	}()

	_, err := s.uploader.Upload(ctx, &s3.PutObjectInput{
		Body:        reader,
		Bucket:      aws.String(s.bucket),
		Key:         aws.String(s3Key),
		ContentType: aws.String(flags.Archiver.MediaType()),
		Metadata:    flags.Metadata,
	}, func(u *s3manager.Uploader) {
		size := flags.S3PartSize * 1024 * 1024
		if size > s3manager.MinUploadPartSize {
			u.PartSize = size
		}
		if flags.S3Thread > 0 {
			u.Concurrency = flags.S3Thread
		}
	})
	if err != nil {
		return err
	}
	return <-errChan
}

func (s S3) Download(ctx context.Context, flags DecompressFlags, s3Key, destination string) (metadata map[string]string, err error) {
	data, err := s.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s3Key),
	})
	if err != nil {
		return nil, err
	}
	if err := Decompress(ctx, data.Body, destination, flags); err != nil {
		return nil, err
	}
	return data.Metadata, nil
}

func (s S3) IsExist(ctx context.Context, s3Key string) (bool, error) {
	_, err := s.s3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s3Key),
	})

	if err != nil {
		if nfe := (*types.NotFound)(nil); errors.As(err, &nfe) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func IsS3(u *url.URL) bool {
	return u.Scheme == "s3"
}
