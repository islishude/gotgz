package gotgz

import (
	"context"
	"io"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type S3 struct {
	uploader *s3manager.Uploader
	s3Client *s3.S3
	bucket   string
}

func New(bucket string) S3 {
	awsSession := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	s3c := s3.New(awsSession)
	return NewWithClient(s3c, bucket)
}

func NewWithClient(s3c *s3.S3, bucket string) S3 {
	return S3{
		uploader: s3manager.NewUploaderWithClient(s3c),
		s3Client: s3c,
		bucket:   bucket,
	}
}

func (s S3) Upload(ctx context.Context, s3Path string, s3Metadata map[string]string, localPath ...string) error {
	reader, writer := io.Pipe()

	errChan := make(chan error, 1)
	go func() {
		errChan <- Compress(writer, localPath...)
		close(errChan)
	}()

	smt := make(map[string]*string, len(s3Metadata))
	for key, value := range s3Metadata {
		smt[key] = aws.String(value)
	}
	_, err := s.uploader.UploadWithContext(ctx, &s3manager.UploadInput{
		Body:        reader,
		Bucket:      aws.String(s.bucket),
		Key:         aws.String(s3Path),
		ContentType: aws.String("application/x-gzip"),
		Metadata:    smt,
	})
	if tgzerr := <-errChan; tgzerr != nil {
		return tgzerr
	}
	return err
}

func (s S3) Download(ctx context.Context, s3Path, localPath string, dflags DecompressFlags) (metadata map[string]string, err error) {
	data, err := s.s3Client.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s3Path),
	})
	if err != nil {
		return nil, err
	}
	if err := Decompress(data.Body, localPath, dflags); err != nil {
		return nil, err
	}

	metadata = make(map[string]string, len(data.Metadata))
	for key, value := range data.Metadata {
		if value != nil {
			metadata[key] = *value
		}
	}
	return metadata, nil
}

func (s S3) IsExist(ctx context.Context, s3Path string) (bool, error) {
	_, err := s.s3Client.HeadObjectWithContext(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s3Path),
	})

	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok && awsErr.Code() == "NotFound" {
			return false, nil
		}
		return false, err
	}

	return true, nil
}
