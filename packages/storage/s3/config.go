package s3

import (
	"context"
	"os"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

// New builds an S3 store using AWS SDK default configuration and gotgz S3
// environment overrides for retry, transfer size, concurrency, and addressing.
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
		o.DisableLogOutputChecksumValidationSkipped = true
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

// intFromEnv parses one integer environment variable and reports whether it
// contained a usable value.
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

// int64FromEnv parses one int64 environment variable and reports whether it
// contained a usable value.
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

// defaultString returns def when v is blank after trimming whitespace.
func defaultString(v, def string) string {
	if strings.TrimSpace(v) == "" {
		return def
	}
	return v
}
