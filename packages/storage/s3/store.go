package s3

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

type rangeGetObjectClient interface {
	GetObject(context.Context, *awss3.GetObjectInput, ...func(*awss3.Options)) (*awss3.GetObjectOutput, error)
}

type Store struct {
	client      *awss3.Client
	rangeClient rangeGetObjectClient
	tm          *transfermanager.Client
	settings    Settings
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
