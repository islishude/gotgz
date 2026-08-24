package s3

import (
	"errors"
	"fmt"
	"io"
	"time"

	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const (
	defaultTransferPartSize    int64 = 16 * 1024 * 1024
	defaultMultipartThreshold  int64 = 16 * 1024 * 1024
	defaultTransferConcurrency       = 4
	defaultTransferAttempts          = 3
	defaultMaxUploadParts      int32 = 10_000
	defaultMaxPartSize         int64 = 5 * 1024 * 1024 * 1024
	defaultMaxObjectSize       int64 = 5 * 1024 * 1024 * 1024 * 1024
	defaultAbortTimeout              = 30 * time.Second
)

type transferOptions struct {
	partSize           int64
	multipartThreshold int64
	concurrency        int
	bodyAttempts       int
	maxUploadParts     int32
	maxPartSize        int64
	maxObjectSize      int64
	abortTimeout       time.Duration
}

type transferManager struct {
	client  transferAPIClient
	options transferOptions
}

func newTransferManager(client transferAPIClient, options transferOptions) *transferManager {
	if options.partSize <= 0 {
		options.partSize = defaultTransferPartSize
	}
	if options.multipartThreshold <= 0 {
		options.multipartThreshold = defaultMultipartThreshold
	}
	if options.concurrency <= 0 {
		options.concurrency = defaultTransferConcurrency
	}
	if options.bodyAttempts <= 0 {
		options.bodyAttempts = defaultTransferAttempts
	}
	if options.maxUploadParts <= 0 {
		options.maxUploadParts = defaultMaxUploadParts
	}
	if options.maxPartSize <= 0 {
		options.maxPartSize = defaultMaxPartSize
	}
	if options.maxObjectSize <= 0 {
		options.maxObjectSize = defaultMaxObjectSize
	}
	if options.abortTimeout <= 0 {
		options.abortTimeout = defaultAbortTimeout
	}
	return &transferManager{client: client, options: options}
}

// uploadRequest is the exact upload surface used by gotgz. cancelRead is set
// only for manager-owned streams such as io.Pipe and never transfers ownership
// of a caller-provided UploadStream reader.
type uploadRequest struct {
	bucket               string
	key                  string
	body                 io.Reader
	metadata             map[string]string
	contentType          *string
	cacheControl         *string
	tagging              *string
	serverSideEncryption s3types.ServerSideEncryption
	sseKMSKeyID          *string
	cancelRead           func(error)
}

type multipartCleanupError struct {
	err        error
	cleanupErr error
}

func newMultipartCleanupError(root, cleanup error) error {
	wrappedCleanup := fmt.Errorf("abort multipart upload: %w", cleanup)
	return &multipartCleanupError{
		err:        errors.Join(root, wrappedCleanup),
		cleanupErr: wrappedCleanup,
	}
}

func (e *multipartCleanupError) Error() string { return e.err.Error() }
func (e *multipartCleanupError) Unwrap() error { return e.err }
func (e *multipartCleanupError) CleanupError() error {
	return e.cleanupErr
}
