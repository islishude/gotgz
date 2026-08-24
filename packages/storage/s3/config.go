package s3

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

// New builds an S3 store using AWS SDK default configuration and gotgz S3
// environment overrides for retry, transfer size, concurrency, and addressing.
func New(ctx context.Context) (*Store, error) {
	settings, retryMax, hasRetryMax, err := settingsFromEnv()
	if err != nil {
		return nil, err
	}
	var cfg aws.Config
	if hasRetryMax {
		cfg, err = config.LoadDefaultConfig(ctx, config.WithRetryMaxAttempts(retryMax))
	} else {
		cfg, err = config.LoadDefaultConfig(ctx)
	}
	if err != nil {
		return nil, err
	}

	client := awss3.NewFromConfig(cfg, func(o *awss3.Options) {
		o.DisableLogOutputChecksumValidationSkipped = true
		o.UsePathStyle = settings.UsePathStyle
	})
	bodyAttempts := defaultTransferAttempts
	if hasRetryMax {
		bodyAttempts = retryMax
	}
	transfers := newTransferManager(client, transferOptions{
		partSize:     settings.PartSizeMB * 1024 * 1024,
		concurrency:  settings.Concurrency,
		bodyAttempts: bodyAttempts,
	})
	return &Store{client: client, transfers: transfers, settings: settings}, nil
}

func settingsFromEnv() (Settings, int, bool, error) {
	settings := Settings{
		PartSizeMB:  16,
		Concurrency: 4,
		SSE:         strings.ToLower(strings.TrimSpace(defaultString(os.Getenv("GOTGZ_S3_SSE"), "AES256"))),
		SSEKMSKeyID: strings.TrimSpace(os.Getenv("GOTGZ_S3_SSE_KMS_KEY_ID")),
	}

	retryMax, hasRetryMax, err := strictIntEnv("GOTGZ_S3_MAX_RETRIES")
	if err != nil {
		return Settings{}, 0, false, err
	}
	if hasRetryMax && retryMax <= 0 {
		return Settings{}, 0, false, fmt.Errorf("GOTGZ_S3_MAX_RETRIES must be greater than zero")
	}

	if partSize, ok, err := strictInt64Env("GOTGZ_S3_PART_SIZE_MB"); err != nil {
		return Settings{}, 0, false, err
	} else if ok {
		if partSize < 5 || partSize > 5120 {
			return Settings{}, 0, false, fmt.Errorf("GOTGZ_S3_PART_SIZE_MB must be between 5 and 5120")
		}
		settings.PartSizeMB = partSize
	}

	if concurrency, ok, err := strictIntEnv("GOTGZ_S3_CONCURRENCY"); err != nil {
		return Settings{}, 0, false, err
	} else if ok {
		if concurrency <= 0 {
			return Settings{}, 0, false, fmt.Errorf("GOTGZ_S3_CONCURRENCY must be greater than zero")
		}
		settings.Concurrency = concurrency
	}

	if raw := strings.TrimSpace(os.Getenv("GOTGZ_S3_USE_PATH_STYLE")); raw != "" {
		value, err := strconv.ParseBool(raw)
		if err != nil {
			return Settings{}, 0, false, fmt.Errorf("GOTGZ_S3_USE_PATH_STYLE must be a boolean: %w", err)
		}
		settings.UsePathStyle = value
	}

	switch settings.SSE {
	case "aes256", "sse-s3", "aws:kms", "sse-kms", "none":
	default:
		return Settings{}, 0, false, fmt.Errorf("GOTGZ_S3_SSE has unsupported value %q", settings.SSE)
	}
	if settings.SSEKMSKeyID != "" && settings.SSE != "aws:kms" && settings.SSE != "sse-kms" {
		return Settings{}, 0, false, fmt.Errorf("GOTGZ_S3_SSE_KMS_KEY_ID requires GOTGZ_S3_SSE=aws:kms or sse-kms")
	}
	return settings, retryMax, hasRetryMax, nil
}

func strictIntEnv(key string) (int, bool, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return 0, false, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, false, fmt.Errorf("%s must be an integer: %w", key, err)
	}
	return value, true, nil
}

func strictInt64Env(key string) (int64, bool, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return 0, false, nil
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, false, fmt.Errorf("%s must be an integer: %w", key, err)
	}
	return value, true, nil
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
