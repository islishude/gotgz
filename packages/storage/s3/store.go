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
	PartSizeMB          int64
	Concurrency         int
	SSE                 string
	SSEKMSKeyID         string
	ExtractWorkers      int
	ExtractStagingBytes int64
	ExtractStagingDir   string
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

// Settings returns a copy of the store configuration derived from environment.
func (s *Store) Settings() Settings {
	return s.settings
}
