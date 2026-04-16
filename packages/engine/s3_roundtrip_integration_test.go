//go:build integration

package engine

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/islishude/gotgz/packages/cli"
)

func TestIntegrationS3MemberCreateLocalArchive(t *testing.T) {
	ctx := context.Background()
	endpoint := integrationS3Endpoint(t)
	client, bucket := setupS3Bucket(t, ctx, endpoint)
	putObject(t, ctx, client, bucket, "data/hello.txt", "hello-world")
	putObject(t, ctx, client, bucket, "data/foo.txt", "foo-bar")

	root := t.TempDir()
	archivePath := filepath.Join(root, "out.tar")
	r := newRunnerWithEndpoint(t, endpoint, io.Discard, io.Discard)
	if got := r.Run(ctx, cli.Options{Mode: cli.ModeCreate, Archive: archivePath, Members: []string{fmt.Sprintf("s3://%s/data/hello.txt", bucket), fmt.Sprintf("s3://%s/data/foo.txt", bucket)}}); got.ExitCode != ExitSuccess {
		t.Fatalf("create exit=%d err=%v", got.ExitCode, got.Err)
	}

	var stdout bytes.Buffer
	listRunner := newRunnerWithEndpoint(t, endpoint, &stdout, io.Discard)
	if got := listRunner.Run(ctx, cli.Options{Mode: cli.ModeList, Archive: archivePath}); got.ExitCode != ExitSuccess {
		t.Fatalf("list exit=%d err=%v", got.ExitCode, got.Err)
	}
	if !strings.Contains(stdout.String(), "data/hello.txt") || !strings.Contains(stdout.String(), "data/foo.txt") {
		t.Fatalf("list output = %q", stdout.String())
	}
}

func TestIntegrationLocalToS3ArchiveToLocalExtract(t *testing.T) {
	ctx := context.Background()
	endpoint := integrationS3Endpoint(t)
	_, bucket := setupS3Bucket(t, ctx, endpoint)

	root := t.TempDir()
	writeFixtureTree(t, root, []fixtureEntry{{path: "src/a.txt", body: "alpha"}, {path: "src/b.txt", body: "bravo"}})
	archiveURI := fmt.Sprintf("s3://%s/archives/test.tar", bucket)
	outDir := filepath.Join(root, "out")
	r := newRunnerWithEndpoint(t, endpoint, io.Discard, io.Discard)
	if got := r.Run(ctx, cli.Options{Mode: cli.ModeCreate, Archive: archiveURI, Chdir: root, Members: []string{"src"}}); got.ExitCode != ExitSuccess {
		t.Fatalf("create exit=%d err=%v", got.ExitCode, got.Err)
	}
	if got := r.Run(ctx, cli.Options{Mode: cli.ModeExtract, Archive: archiveURI, Chdir: outDir}); got.ExitCode != ExitSuccess {
		t.Fatalf("extract exit=%d err=%v", got.ExitCode, got.Err)
	}
	if mustReadFile(t, filepath.Join(outDir, "src", "a.txt")) != "alpha" || mustReadFile(t, filepath.Join(outDir, "src", "b.txt")) != "bravo" {
		t.Fatal("local extract did not restore expected files")
	}
}

func TestIntegrationSplitArchiveToFromS3(t *testing.T) {
	tests := []struct {
		name            string
		archiveName     string
		wantContentType string
	}{
		{name: "tar.gz", archiveName: "split.tar.gz", wantContentType: "application/gzip"},
		{name: "zip", archiveName: "split.zip", wantContentType: "application/zip"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			endpoint := integrationS3Endpoint(t)
			client, bucket := setupS3Bucket(t, ctx, endpoint)

			root := t.TempDir()
			writeFixtureTree(t, root, []fixtureEntry{{path: "src/one.txt", body: "one"}, {path: "src/two.txt", body: "two"}})
			archiveURI := fmt.Sprintf("s3://%s/archives/%s", bucket, tt.archiveName)
			outDir := filepath.Join(root, "out")
			r := newRunnerWithEndpoint(t, endpoint, io.Discard, io.Discard)
			if got := r.Run(ctx, cli.Options{Mode: cli.ModeCreate, Archive: archiveURI, Chdir: root, Members: []string{"src"}, SplitSizeBytes: 1, Compression: compressionForArchive(tt.archiveName)}); got.ExitCode != ExitSuccess {
				t.Fatalf("create exit=%d err=%v", got.ExitCode, got.Err)
			}

			firstKey := strings.Replace(tt.archiveName, ".", ".part0001.", 1)
			secondKey := strings.Replace(tt.archiveName, ".", ".part0002.", 1)
			if tt.archiveName == "split.tar.gz" {
				firstKey = "split.part0001.tar.gz"
				secondKey = "split.part0002.tar.gz"
			}
			if got := aws.ToString(headObject(t, ctx, client, bucket, "archives/"+firstKey).ContentType); got != tt.wantContentType {
				t.Fatalf("content-type = %q, want %q", got, tt.wantContentType)
			}
			_ = headObject(t, ctx, client, bucket, "archives/"+secondKey)

			if got := r.Run(ctx, cli.Options{Mode: cli.ModeExtract, Archive: fmt.Sprintf("s3://%s/archives/%s", bucket, firstKey), Chdir: outDir}); got.ExitCode != ExitSuccess {
				t.Fatalf("extract exit=%d err=%v", got.ExitCode, got.Err)
			}
			if mustReadFile(t, filepath.Join(outDir, "src", "one.txt")) != "one" || mustReadFile(t, filepath.Join(outDir, "src", "two.txt")) != "two" {
				t.Fatal("split S3 roundtrip did not restore files")
			}
		})
	}
}
