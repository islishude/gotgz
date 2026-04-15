package s3

import (
	"os"
	"path/filepath"
	"testing"
)

func TestParseSettingsFromEnvDefaults(t *testing.T) {
	t.Setenv("GOTGZ_S3_PART_SIZE_MB", "")
	t.Setenv("GOTGZ_S3_CONCURRENCY", "")
	t.Setenv("GOTGZ_S3_SSE", "")
	t.Setenv("GOTGZ_S3_SSE_KMS_KEY_ID", "")
	t.Setenv("GOTGZ_S3_EXTRACT_WORKERS", "")
	t.Setenv("GOTGZ_S3_EXTRACT_STAGING_BYTES", "")
	t.Setenv("GOTGZ_S3_EXTRACT_STAGING_DIR", "")

	got := parseSettingsFromEnv()
	if got.PartSizeMB != defaultPartSizeMB {
		t.Fatalf("PartSizeMB = %d, want %d", got.PartSizeMB, defaultPartSizeMB)
	}
	if got.Concurrency != defaultConcurrency {
		t.Fatalf("Concurrency = %d, want %d", got.Concurrency, defaultConcurrency)
	}
	if got.SSE != "aes256" {
		t.Fatalf("SSE = %q, want %q", got.SSE, "aes256")
	}
	if got.ExtractWorkers != defaultExtractWorkers {
		t.Fatalf("ExtractWorkers = %d, want %d", got.ExtractWorkers, defaultExtractWorkers)
	}
	if got.ExtractStagingBytes != defaultExtractStagingBytes {
		t.Fatalf("ExtractStagingBytes = %d, want %d", got.ExtractStagingBytes, defaultExtractStagingBytes)
	}
	if got.ExtractStagingDir != filepath.Clean(os.TempDir()) {
		t.Fatalf("ExtractStagingDir = %q, want %q", got.ExtractStagingDir, filepath.Clean(os.TempDir()))
	}
}

func TestParseSettingsFromEnvOverrides(t *testing.T) {
	t.Setenv("GOTGZ_S3_PART_SIZE_MB", "32")
	t.Setenv("GOTGZ_S3_CONCURRENCY", "12")
	t.Setenv("GOTGZ_S3_SSE", " aws:kms ")
	t.Setenv("GOTGZ_S3_SSE_KMS_KEY_ID", " key-id ")
	t.Setenv("GOTGZ_S3_EXTRACT_WORKERS", "3")
	t.Setenv("GOTGZ_S3_EXTRACT_STAGING_BYTES", "1048576")
	t.Setenv("GOTGZ_S3_EXTRACT_STAGING_DIR", " /tmp/gotgz-stage ")

	got := parseSettingsFromEnv()
	if got.PartSizeMB != 32 {
		t.Fatalf("PartSizeMB = %d, want 32", got.PartSizeMB)
	}
	if got.Concurrency != 12 {
		t.Fatalf("Concurrency = %d, want 12", got.Concurrency)
	}
	if got.SSE != "aws:kms" {
		t.Fatalf("SSE = %q, want %q", got.SSE, "aws:kms")
	}
	if got.SSEKMSKeyID != "key-id" {
		t.Fatalf("SSEKMSKeyID = %q, want %q", got.SSEKMSKeyID, "key-id")
	}
	if got.ExtractWorkers != 3 {
		t.Fatalf("ExtractWorkers = %d, want 3", got.ExtractWorkers)
	}
	if got.ExtractStagingBytes != 1048576 {
		t.Fatalf("ExtractStagingBytes = %d, want 1048576", got.ExtractStagingBytes)
	}
	if got.ExtractStagingDir != "/tmp/gotgz-stage" {
		t.Fatalf("ExtractStagingDir = %q, want %q", got.ExtractStagingDir, "/tmp/gotgz-stage")
	}
}
