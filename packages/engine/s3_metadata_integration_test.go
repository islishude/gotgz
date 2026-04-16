//go:build integration

package engine

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/islishude/gotgz/packages/cli"
)

func TestIntegrationS3ArchiveUploadMetadata(t *testing.T) {
	tests := []struct {
		name            string
		archiveName     string
		wantContentType string
	}{
		{name: "tar.gz", archiveName: "with-metadata.tar.gz", wantContentType: "application/gzip"},
		{name: "zip", archiveName: "with-metadata.zip", wantContentType: "application/zip"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			endpoint := integrationS3Endpoint(t)
			client, bucket := setupS3Bucket(t, ctx, endpoint)
			root := t.TempDir()
			writeFixtureTree(t, root, []fixtureEntry{{path: "src/msg.txt", body: "payload"}})

			archiveURI := fmt.Sprintf("s3://%s/archives/%s?env=prod", bucket, tt.archiveName)
			r := newRunnerWithEndpoint(t, endpoint, io.Discard, io.Discard)
			got := r.Run(ctx, cli.Options{Mode: cli.ModeCreate, Archive: archiveURI, Chdir: root, Members: []string{"src"}, Compression: compressionForArchive(tt.archiveName), S3CacheControl: "max-age=300,public", S3ObjectTags: map[string]string{"team": "archive"}})
			if got.ExitCode != ExitSuccess {
				t.Fatalf("create exit=%d err=%v", got.ExitCode, got.Err)
			}

			head := headObject(t, ctx, client, bucket, "archives/"+tt.archiveName)
			if aws.ToString(head.ContentType) != tt.wantContentType {
				t.Fatalf("ContentType = %q, want %q", aws.ToString(head.ContentType), tt.wantContentType)
			}
			if aws.ToString(head.CacheControl) != "max-age=300,public" {
				t.Fatalf("CacheControl = %q", aws.ToString(head.CacheControl))
			}
			if head.Metadata["env"] != "prod" {
				t.Fatalf("env metadata = %q, want prod", head.Metadata["env"])
			}
			tags := getObjectTags(t, ctx, client, bucket, "archives/"+tt.archiveName)
			if tags["team"] != "archive" {
				t.Fatalf("tags[team] = %q, want archive", tags["team"])
			}
		})
	}
}

func TestIntegrationExtractToS3MetadataPropagation(t *testing.T) {
	tests := []struct {
		name        string
		archiveName string
		payload     []byte
		objectKey   string
	}{
		{name: "tar", archiveName: "input.tar", payload: tarArchiveBytes(t, map[string]string{"dir/file.txt": "payload"}), objectKey: "extracted/dir/file.txt"},
		{name: "zip", archiveName: "input.zip", payload: zipArchiveBytes(t, map[string]string{"dir/file.txt": "payload"}), objectKey: "extracted/dir/file.txt"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			endpoint := integrationS3Endpoint(t)
			client, bucket := setupS3Bucket(t, ctx, endpoint)
			root := t.TempDir()
			archivePath := filepath.Join(root, tt.archiveName)
			if err := os.WriteFile(archivePath, tt.payload, 0o644); err != nil {
				t.Fatalf("WriteFile() error = %v", err)
			}

			r := newRunnerWithEndpoint(t, endpoint, io.Discard, io.Discard)
			got := r.Run(ctx, cli.Options{Mode: cli.ModeExtract, Archive: archivePath, Chdir: fmt.Sprintf("s3://%s/extracted/", bucket), S3CacheControl: "no-store", S3ObjectTags: map[string]string{"team": "restore"}})
			if got.ExitCode != ExitSuccess {
				t.Fatalf("extract exit=%d err=%v", got.ExitCode, got.Err)
			}

			head := headObject(t, ctx, client, bucket, tt.objectKey)
			if aws.ToString(head.CacheControl) != "no-store" {
				t.Fatalf("CacheControl = %q, want no-store", aws.ToString(head.CacheControl))
			}
			tags := getObjectTags(t, ctx, client, bucket, tt.objectKey)
			if tags["team"] != "restore" {
				t.Fatalf("tags[team] = %q, want restore", tags["team"])
			}
			if got := getObject(t, ctx, client, bucket, tt.objectKey); got != "payload" {
				t.Fatalf("object body = %q, want payload", got)
			}
		})
	}
}
