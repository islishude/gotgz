package s3

import (
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	tmtypes "github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager/types"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/islishude/gotgz/internal/locator"
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
	Size int64
	ETag string
}

func New(ctx context.Context) (*Store, error) {
	retryMax, ok := intFromEnv("GOTGZ_S3_MAX_RETRIES")
	var cfg aws.Config
	var err error
	if ok {
		cfg, err = config.LoadDefaultConfig(ctx, config.WithRetryMaxAttempts(retryMax))
	} else {
		cfg, err = config.LoadDefaultConfig(ctx)
	}
	if err != nil {
		return nil, err
	}

	settings := Settings{
		PartSizeMB:  16,
		Concurrency: 4,
		SSE:         strings.ToLower(strings.TrimSpace(defaultString(os.Getenv("GOTGZ_S3_SSE"), "AES256"))),
		SSEKMSKeyID: strings.TrimSpace(os.Getenv("GOTGZ_S3_SSE_KMS_KEY_ID")),
	}
	if v, ok := int64FromEnv("GOTGZ_S3_PART_SIZE_MB"); ok && v > 0 {
		settings.PartSizeMB = v
	}
	if v, ok := intFromEnv("GOTGZ_S3_CONCURRENCY"); ok && v > 0 {
		settings.Concurrency = v
	}

	client := awss3.NewFromConfig(cfg, func(o *awss3.Options) {
		if strings.EqualFold(strings.TrimSpace(os.Getenv("GOTGZ_S3_USE_PATH_STYLE")), "true") {
			o.UsePathStyle = true
		}
	})
	tm := transfermanager.New(client, func(o *transfermanager.Options) {
		o.PartSizeBytes = settings.PartSizeMB * 1024 * 1024
		o.Concurrency = settings.Concurrency
	})
	return &Store{client: client, tm: tm, settings: settings}, nil
}

func (s *Store) OpenReader(ctx context.Context, ref locator.Ref) (io.ReadCloser, Metadata, error) {
	if ref.Kind != locator.KindS3 {
		return nil, Metadata{}, fmt.Errorf("ref %q is not s3", ref.Raw)
	}
	out, err := s.client.GetObject(ctx, &awss3.GetObjectInput{Bucket: aws.String(ref.Bucket), Key: aws.String(ref.Key)})
	if err != nil {
		return nil, Metadata{}, err
	}
	meta := Metadata{Size: aws.ToInt64(out.ContentLength), ETag: aws.ToString(out.ETag)}
	return out.Body, meta, nil
}

func (s *Store) OpenWriter(ctx context.Context, ref locator.Ref, metadata map[string]string) (io.WriteCloser, error) {
	if ref.Kind != locator.KindS3 {
		return nil, fmt.Errorf("ref %q is not s3", ref.Raw)
	}
	pr, pw := io.Pipe()
	errCh := make(chan error, 1)
	in := &transfermanager.UploadObjectInput{
		Bucket:   aws.String(ref.Bucket),
		Key:      aws.String(ref.Key),
		Body:     pr,
		Metadata: metadata,
	}
	s.applyEncryption(in)
	go func() {
		_, err := s.tm.UploadObject(ctx, in)
		_ = pr.Close()
		errCh <- err
		close(errCh)
	}()
	return &uploadWriter{pw: pw, errCh: errCh}, nil
}

func (s *Store) UploadStream(ctx context.Context, ref locator.Ref, body io.Reader, metadata map[string]string) error {
	if ref.Kind != locator.KindS3 {
		return fmt.Errorf("ref %q is not s3", ref.Raw)
	}
	in := &transfermanager.UploadObjectInput{
		Bucket:   aws.String(ref.Bucket),
		Key:      aws.String(ref.Key),
		Body:     body,
		Metadata: metadata,
	}
	s.applyEncryption(in)
	_, err := s.tm.UploadObject(ctx, in)
	return err
}

func (s *Store) applyEncryption(in *transfermanager.UploadObjectInput) {
	switch s.settings.SSE {
	case "", "aes256", "sse-s3":
		in.ServerSideEncryption = tmtypes.ServerSideEncryptionAes256
	case "aws:kms", "sse-kms":
		in.ServerSideEncryption = tmtypes.ServerSideEncryptionAwsKms
		if s.settings.SSEKMSKeyID != "" {
			in.SSEKMSKeyID = aws.String(s.settings.SSEKMSKeyID)
		}
	case "none":
		return
	default:
		in.ServerSideEncryption = tmtypes.ServerSideEncryptionAes256
	}
}

type uploadWriter struct {
	pw    *io.PipeWriter
	errCh <-chan error
}

func (w *uploadWriter) Write(p []byte) (int, error) {
	return w.pw.Write(p)
}

func (w *uploadWriter) Close() error {
	if err := w.pw.Close(); err != nil {
		return err
	}
	if err, ok := <-w.errCh; ok && err != nil {
		return err
	}
	return nil
}

func intFromEnv(key string) (int, bool) {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return 0, false
	}
	x, err := strconv.Atoi(v)
	if err != nil {
		return 0, false
	}
	return x, true
}

func int64FromEnv(key string) (int64, bool) {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return 0, false
	}
	x, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0, false
	}
	return x, true
}

func defaultString(v, def string) string {
	if strings.TrimSpace(v) == "" {
		return def
	}
	return v
}
