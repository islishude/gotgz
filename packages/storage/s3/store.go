package s3

import (
	"context"
	"time"

	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

type transferAPIClient interface {
	PutObject(context.Context, *awss3.PutObjectInput, ...func(*awss3.Options)) (*awss3.PutObjectOutput, error)
	CreateMultipartUpload(context.Context, *awss3.CreateMultipartUploadInput, ...func(*awss3.Options)) (*awss3.CreateMultipartUploadOutput, error)
	UploadPart(context.Context, *awss3.UploadPartInput, ...func(*awss3.Options)) (*awss3.UploadPartOutput, error)
	CompleteMultipartUpload(context.Context, *awss3.CompleteMultipartUploadInput, ...func(*awss3.Options)) (*awss3.CompleteMultipartUploadOutput, error)
	AbortMultipartUpload(context.Context, *awss3.AbortMultipartUploadInput, ...func(*awss3.Options)) (*awss3.AbortMultipartUploadOutput, error)
	GetObject(context.Context, *awss3.GetObjectInput, ...func(*awss3.Options)) (*awss3.GetObjectOutput, error)
	HeadObject(context.Context, *awss3.HeadObjectInput, ...func(*awss3.Options)) (*awss3.HeadObjectOutput, error)
}

type s3APIClient interface {
	transferAPIClient
	ListObjectsV2(context.Context, *awss3.ListObjectsV2Input, ...func(*awss3.Options)) (*awss3.ListObjectsV2Output, error)
}

type Store struct {
	client    s3APIClient
	transfers *transferManager
	settings  Settings
}

type Settings struct {
	PartSizeMB   int64
	Concurrency  int
	SSE          string
	SSEKMSKeyID  string
	UsePathStyle bool
}

type Metadata struct {
	Size         int64
	ContentType  string
	ETag         string
	VersionID    string
	LastModified time.Time
}

// ListedObject describes an object discovered while enumerating a prefix.
type ListedObject struct {
	Key  string
	Size int64
}
