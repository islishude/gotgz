package s3

import (
	"strings"
	"testing"
)

// TestIntFromEnv verifies that integer environment settings are parsed only
// when present and valid.
func TestIntFromEnv(t *testing.T) {
	t.Setenv("GOTGZ_TEST_INT", " 42 ")
	if got, ok := intFromEnv("GOTGZ_TEST_INT"); !ok || got != 42 {
		t.Fatalf("intFromEnv(valid) = (%d, %t), want (42, true)", got, ok)
	}

	t.Setenv("GOTGZ_TEST_INT", "bad")
	if got, ok := intFromEnv("GOTGZ_TEST_INT"); ok || got != 0 {
		t.Fatalf("intFromEnv(invalid) = (%d, %t), want (0, false)", got, ok)
	}

	t.Setenv("GOTGZ_TEST_INT", "")
	if got, ok := intFromEnv("GOTGZ_TEST_INT"); ok || got != 0 {
		t.Fatalf("intFromEnv(empty) = (%d, %t), want (0, false)", got, ok)
	}
}

// TestInt64FromEnv verifies that int64 environment settings are parsed only
// when present and valid.
func TestInt64FromEnv(t *testing.T) {
	t.Setenv("GOTGZ_TEST_INT64", " 4096 ")
	if got, ok := int64FromEnv("GOTGZ_TEST_INT64"); !ok || got != 4096 {
		t.Fatalf("int64FromEnv(valid) = (%d, %t), want (4096, true)", got, ok)
	}

	t.Setenv("GOTGZ_TEST_INT64", "bad")
	if got, ok := int64FromEnv("GOTGZ_TEST_INT64"); ok || got != 0 {
		t.Fatalf("int64FromEnv(invalid) = (%d, %t), want (0, false)", got, ok)
	}

	t.Setenv("GOTGZ_TEST_INT64", "")
	if got, ok := int64FromEnv("GOTGZ_TEST_INT64"); ok || got != 0 {
		t.Fatalf("int64FromEnv(empty) = (%d, %t), want (0, false)", got, ok)
	}
}

func TestSettingsFromEnvStrictValidation(t *testing.T) {
	keys := []string{
		"GOTGZ_S3_MAX_RETRIES", "GOTGZ_S3_PART_SIZE_MB", "GOTGZ_S3_CONCURRENCY",
		"GOTGZ_S3_USE_PATH_STYLE", "GOTGZ_S3_SSE", "GOTGZ_S3_SSE_KMS_KEY_ID",
	}
	clear := func(t *testing.T) {
		t.Helper()
		for _, key := range keys {
			t.Setenv(key, "")
		}
	}

	t.Run("valid", func(t *testing.T) {
		clear(t)
		t.Setenv("GOTGZ_S3_MAX_RETRIES", "5")
		t.Setenv("GOTGZ_S3_PART_SIZE_MB", "32")
		t.Setenv("GOTGZ_S3_CONCURRENCY", "8")
		t.Setenv("GOTGZ_S3_USE_PATH_STYLE", "true")
		t.Setenv("GOTGZ_S3_SSE", "aws:kms")
		t.Setenv("GOTGZ_S3_SSE_KMS_KEY_ID", "key-id")
		settings, retries, hasRetries, err := settingsFromEnv()
		if err != nil {
			t.Fatalf("settingsFromEnv() error = %v", err)
		}
		if !hasRetries || retries != 5 || settings.PartSizeMB != 32 || settings.Concurrency != 8 || !settings.UsePathStyle || settings.SSE != "aws:kms" {
			t.Fatalf("settings=%+v retries=%d hasRetries=%v", settings, retries, hasRetries)
		}
	})

	tests := []struct {
		name  string
		key   string
		value string
		want  string
	}{
		{name: "bad retries", key: "GOTGZ_S3_MAX_RETRIES", value: "bad", want: "must be an integer"},
		{name: "zero retries", key: "GOTGZ_S3_MAX_RETRIES", value: "0", want: "greater than zero"},
		{name: "small part", key: "GOTGZ_S3_PART_SIZE_MB", value: "4", want: "between 5 and 5120"},
		{name: "large part", key: "GOTGZ_S3_PART_SIZE_MB", value: "5121", want: "between 5 and 5120"},
		{name: "zero concurrency", key: "GOTGZ_S3_CONCURRENCY", value: "0", want: "greater than zero"},
		{name: "bad bool", key: "GOTGZ_S3_USE_PATH_STYLE", value: "sometimes", want: "must be a boolean"},
		{name: "bad sse", key: "GOTGZ_S3_SSE", value: "kms-typo", want: "unsupported value"},
		{name: "kms key without kms", key: "GOTGZ_S3_SSE_KMS_KEY_ID", value: "key-id", want: "requires GOTGZ_S3_SSE"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clear(t)
			t.Setenv(tt.key, tt.value)
			_, _, _, err := settingsFromEnv()
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("settingsFromEnv() error = %v, want %q", err, tt.want)
			}
		})
	}
}
