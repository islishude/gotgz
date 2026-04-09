package s3

import (
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

type Store struct {
	client   *awss3.Client
	tm       *transfermanager.Client
	settings Settings
}

type Settings struct {
	PartSizeMB  int64
	Concurrency int
	SSE         string
	SSEKMSKeyID string
}

type Metadata struct {
	Size        int64
	ContentType string
}

// ListedObject describes an object discovered while enumerating a prefix.
type ListedObject struct {
	Key  string
	Size int64
}
