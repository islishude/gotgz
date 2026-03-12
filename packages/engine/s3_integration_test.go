package engine

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/islishude/gotgz/packages/cli"
	"github.com/islishude/gotgz/packages/locator"
)

// s3Endpoint returns the configured S3 emulator endpoint, or skips the test.
func s3Endpoint(t *testing.T) string {
	t.Helper()
	ep := os.Getenv("GOTGZ_TEST_S3_ENDPOINT")
	if ep == "" {
		t.Skip("GOTGZ_TEST_S3_ENDPOINT not set; skipping S3 integration test")
	}
	return ep
}

// setupS3Bucket creates a temporary bucket on the configured S3 emulator and returns its name.
// The bucket is deleted when the test finishes.
func setupS3Bucket(t *testing.T, ctx context.Context, endpoint string) (*awss3.Client, string) {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(aws.CredentialsProviderFunc(
			func(ctx context.Context) (aws.Credentials, error) {
				return aws.Credentials{AccessKeyID: "test", SecretAccessKey: "test"}, nil
			})),
	)
	if err != nil {
		t.Fatalf("load aws config: %v", err)
	}

	client := awss3.NewFromConfig(cfg, func(o *awss3.Options) {
		o.BaseEndpoint = new(endpoint)
		o.UsePathStyle = true
	})

	bucket := fmt.Sprintf("gotgz-test-%d", os.Getpid())
	_, err = client.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: new(bucket)})
	if err != nil {
		t.Fatalf("create bucket %s: %v", bucket, err)
	}

	t.Cleanup(func() {
		// Delete all objects then the bucket.
		list, _ := client.ListObjectsV2(ctx, &awss3.ListObjectsV2Input{Bucket: new(bucket)})
		if list != nil {
			for _, obj := range list.Contents {
				_, _ = client.DeleteObject(ctx, &awss3.DeleteObjectInput{Bucket: new(bucket), Key: obj.Key})
			}
		}
		_, _ = client.DeleteBucket(ctx, &awss3.DeleteBucketInput{Bucket: new(bucket)})
	})
	return client, bucket
}

// putObject uploads an object to the configured S3 emulator.
func putObject(t *testing.T, ctx context.Context, client *awss3.Client, bucket, key, body string) {
	t.Helper()
	_, err := client.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: new(bucket),
		Key:    new(key),
		Body:   strings.NewReader(body),
	})
	if err != nil {
		t.Fatalf("put s3://%s/%s: %v", bucket, key, err)
	}
}

// getObject reads an S3 object and returns its body as a string.
func getObject(t *testing.T, ctx context.Context, client *awss3.Client, bucket, key string) string {
	t.Helper()
	out, err := client.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: new(bucket),
		Key:    new(key),
	})
	if err != nil {
		t.Fatalf("get s3://%s/%s: %v", bucket, key, err)
	}
	defer out.Body.Close() // nolint: errcheck
	b, err := io.ReadAll(out.Body)
	if err != nil {
		t.Fatalf("read s3://%s/%s: %v", bucket, key, err)
	}
	return string(b)
}

// getObjectTags reads the tag set for one S3 object into a key-value map.
func getObjectTags(t *testing.T, ctx context.Context, client *awss3.Client, bucket, key string) map[string]string {
	t.Helper()
	out, err := client.GetObjectTagging(ctx, &awss3.GetObjectTaggingInput{
		Bucket: new(bucket),
		Key:    new(key),
	})
	if err != nil {
		t.Fatalf("get tags for s3://%s/%s: %v", bucket, key, err)
	}
	tags := make(map[string]string, len(out.TagSet))
	for _, tag := range out.TagSet {
		tags[aws.ToString(tag.Key)] = aws.ToString(tag.Value)
	}
	return tags
}

// newRunnerWithEndpoint creates a Runner pointing at the given S3 endpoint.
func newRunnerWithEndpoint(t *testing.T, endpoint string, stdout, stderr io.Writer) *Runner {
	t.Helper()
	// Point the AWS SDK at the configured S3 emulator via environment.
	t.Setenv("AWS_ENDPOINT_URL", endpoint)
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("GOTGZ_S3_SSE", "none")
	t.Setenv("GOTGZ_S3_USE_PATH_STYLE", "true")

	r, err := New(context.Background(), stdout, stderr)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return r
}

// ---------------------------------------------------------------------------
// Test: create a local archive from S3 members
// ---------------------------------------------------------------------------
func TestS3MemberCreateLocalArchive(t *testing.T) {
	ctx := context.Background()
	ep := s3Endpoint(t)
	client, bucket := setupS3Bucket(t, ctx, ep)

	putObject(t, ctx, client, bucket, "data/hello.txt", "hello-world")
	putObject(t, ctx, client, bucket, "data/foo.txt", "foo-bar")

	root := t.TempDir()
	archive := filepath.Join(root, "out.tar")

	r := newRunnerWithEndpoint(t, ep, io.Discard, io.Discard)

	create := cli.Options{
		Mode:    cli.ModeCreate,
		Archive: archive,
		Members: []string{
			fmt.Sprintf("s3://%s/data/hello.txt", bucket),
			fmt.Sprintf("s3://%s/data/foo.txt", bucket),
		},
	}
	res := r.Run(ctx, create)
	if res.ExitCode != ExitSuccess {
		t.Fatalf("create exit=%d err=%v", res.ExitCode, res.Err)
	}

	// List the archive and verify both members are present.
	var listBuf bytes.Buffer
	r2 := newRunnerWithEndpoint(t, ep, &listBuf, io.Discard)
	list := cli.Options{Mode: cli.ModeList, Archive: archive}
	res = r2.Run(ctx, list)
	if res.ExitCode != ExitSuccess {
		t.Fatalf("list exit=%d err=%v", res.ExitCode, res.Err)
	}

	output := listBuf.String()
	if !strings.Contains(output, "data/hello.txt") {
		t.Fatalf("listing missing hello.txt:\n%s", output)
	}
	if !strings.Contains(output, "data/foo.txt") {
		t.Fatalf("listing missing foo.txt:\n%s", output)
	}

	// Extract and verify contents.
	outDir := filepath.Join(root, "extracted")
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		t.Fatal(err)
	}
	r3 := newRunnerWithEndpoint(t, ep, io.Discard, io.Discard)
	extract := cli.Options{Mode: cli.ModeExtract, Archive: archive, Chdir: outDir}
	res = r3.Run(ctx, extract)
	if res.ExitCode != ExitSuccess {
		t.Fatalf("extract exit=%d err=%v", res.ExitCode, res.Err)
	}

	b, err := os.ReadFile(filepath.Join(outDir, "data", "hello.txt"))
	if err != nil {
		t.Fatalf("read extracted hello.txt: %v", err)
	}
	if string(b) != "hello-world" {
		t.Fatalf("content mismatch: %q", string(b))
	}
}

// ---------------------------------------------------------------------------
// Test: create an archive directly to S3
// ---------------------------------------------------------------------------
func TestS3ArchiveCreateToS3(t *testing.T) {
	ctx := context.Background()
	ep := s3Endpoint(t)
	_, bucket := setupS3Bucket(t, ctx, ep)

	root := t.TempDir()
	srcDir := filepath.Join(root, "src")
	if err := os.MkdirAll(srcDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(srcDir, "a.txt"), []byte("alpha"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(srcDir, "b.txt"), []byte("bravo"), 0o644); err != nil {
		t.Fatal(err)
	}

	archiveURI := fmt.Sprintf("s3://%s/archives/test.tar", bucket)

	r := newRunnerWithEndpoint(t, ep, io.Discard, io.Discard)
	create := cli.Options{
		Mode:    cli.ModeCreate,
		Archive: archiveURI,
		Chdir:   root,
		Members: []string{"src"},
	}
	res := r.Run(ctx, create)
	if res.ExitCode != ExitSuccess {
		t.Fatalf("create exit=%d err=%v", res.ExitCode, res.Err)
	}

	// Extract the S3 archive to a local directory to verify.
	outDir := filepath.Join(root, "extracted")
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		t.Fatal(err)
	}
	r2 := newRunnerWithEndpoint(t, ep, io.Discard, io.Discard)
	extract := cli.Options{Mode: cli.ModeExtract, Archive: archiveURI, Chdir: outDir}
	res = r2.Run(ctx, extract)
	if res.ExitCode != ExitSuccess {
		t.Fatalf("extract exit=%d err=%v", res.ExitCode, res.Err)
	}

	for _, tc := range []struct {
		file, want string
	}{
		{"src/a.txt", "alpha"},
		{"src/b.txt", "bravo"},
	} {
		b, err := os.ReadFile(filepath.Join(outDir, tc.file))
		if err != nil {
			t.Fatalf("read %s: %v", tc.file, err)
		}
		if string(b) != tc.want {
			t.Fatalf("%s content = %q, want %q", tc.file, string(b), tc.want)
		}
	}
}

func TestS3SplitArchiveRoundTrip(t *testing.T) {
	ctx := context.Background()
	ep := s3Endpoint(t)
	client, bucket := setupS3Bucket(t, ctx, ep)

	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "one.txt"), []byte("one"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "two.txt"), []byte("two"), 0o644); err != nil {
		t.Fatal(err)
	}

	archiveURI := fmt.Sprintf("s3://%s/archives/split.tar.gz", bucket)

	r := newRunnerWithEndpoint(t, ep, io.Discard, io.Discard)
	create := cli.Options{
		Mode:           cli.ModeCreate,
		Archive:        archiveURI,
		Chdir:          root,
		Compression:    cli.CompressionGzip,
		SplitSizeBytes: 1,
		Members:        []string{"one.txt", "two.txt"},
	}
	res := r.Run(ctx, create)
	if res.ExitCode != ExitSuccess {
		t.Fatalf("create exit=%d err=%v", res.ExitCode, res.Err)
	}

	list, err := client.ListObjectsV2(ctx, &awss3.ListObjectsV2Input{
		Bucket: new(bucket),
		Prefix: new("archives/split.part"),
	})
	if err != nil {
		t.Fatalf("list split objects: %v", err)
	}
	if len(list.Contents) != 2 {
		t.Fatalf("split object count = %d, want 2", len(list.Contents))
	}

	firstKey := "archives/split.part0001.tar.gz"
	secondKey := "archives/split.part0002.tar.gz"
	var foundFirst, foundSecond bool
	for _, obj := range list.Contents {
		switch aws.ToString(obj.Key) {
		case firstKey:
			foundFirst = true
		case secondKey:
			foundSecond = true
		}
	}
	if !foundFirst || !foundSecond {
		t.Fatalf("split objects missing keys: first=%v second=%v", foundFirst, foundSecond)
	}

	head, err := client.HeadObject(ctx, &awss3.HeadObjectInput{
		Bucket: new(bucket),
		Key:    new(firstKey),
	})
	if err != nil {
		t.Fatalf("head split object: %v", err)
	}
	if got := aws.ToString(head.ContentType); got != "application/gzip" {
		t.Fatalf("content type = %q, want %q", got, "application/gzip")
	}

	out := filepath.Join(root, "out")
	if err := os.MkdirAll(out, 0o755); err != nil {
		t.Fatal(err)
	}
	rExtract := newRunnerWithEndpoint(t, ep, io.Discard, io.Discard)
	extract := cli.Options{
		Mode:    cli.ModeExtract,
		Archive: fmt.Sprintf("s3://%s/%s", bucket, firstKey),
		Chdir:   out,
	}
	res = rExtract.Run(ctx, extract)
	if res.ExitCode != ExitSuccess {
		t.Fatalf("extract exit=%d err=%v", res.ExitCode, res.Err)
	}

	for _, tc := range []struct {
		name string
		want string
	}{
		{name: "one.txt", want: "one"},
		{name: "two.txt", want: "two"},
	} {
		b, err := os.ReadFile(filepath.Join(out, tc.name))
		if err != nil {
			t.Fatalf("read %s: %v", tc.name, err)
		}
		if string(b) != tc.want {
			t.Fatalf("%s = %q, want %q", tc.name, string(b), tc.want)
		}
	}
}

func TestS3SplitZipArchiveRoundTrip(t *testing.T) {
	ctx := context.Background()
	ep := s3Endpoint(t)
	client, bucket := setupS3Bucket(t, ctx, ep)

	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "one.txt"), []byte("one"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "two.txt"), []byte("two"), 0o644); err != nil {
		t.Fatal(err)
	}

	archiveURI := fmt.Sprintf("s3://%s/archives/split.zip", bucket)

	r := newRunnerWithEndpoint(t, ep, io.Discard, io.Discard)
	create := cli.Options{
		Mode:           cli.ModeCreate,
		Archive:        archiveURI,
		Chdir:          root,
		SplitSizeBytes: 1,
		Members:        []string{"one.txt", "two.txt"},
	}
	res := r.Run(ctx, create)
	if res.ExitCode != ExitSuccess {
		t.Fatalf("create exit=%d err=%v", res.ExitCode, res.Err)
	}

	list, err := client.ListObjectsV2(ctx, &awss3.ListObjectsV2Input{
		Bucket: new(bucket),
		Prefix: new("archives/split.part"),
	})
	if err != nil {
		t.Fatalf("list split objects: %v", err)
	}
	if len(list.Contents) != 2 {
		t.Fatalf("split object count = %d, want 2", len(list.Contents))
	}

	firstKey := "archives/split.part0001.zip"
	secondKey := "archives/split.part0002.zip"
	var foundFirst, foundSecond bool
	for _, obj := range list.Contents {
		switch aws.ToString(obj.Key) {
		case firstKey:
			foundFirst = true
		case secondKey:
			foundSecond = true
		}
	}
	if !foundFirst || !foundSecond {
		t.Fatalf("split objects missing keys: first=%v second=%v", foundFirst, foundSecond)
	}

	head, err := client.HeadObject(ctx, &awss3.HeadObjectInput{
		Bucket: new(bucket),
		Key:    new(firstKey),
	})
	if err != nil {
		t.Fatalf("head split object: %v", err)
	}
	if got := aws.ToString(head.ContentType); got != "application/zip" {
		t.Fatalf("content type = %q, want %q", got, "application/zip")
	}

	out := filepath.Join(root, "out")
	if err := os.MkdirAll(out, 0o755); err != nil {
		t.Fatal(err)
	}
	rExtract := newRunnerWithEndpoint(t, ep, io.Discard, io.Discard)
	extract := cli.Options{
		Mode:    cli.ModeExtract,
		Archive: fmt.Sprintf("s3://%s/%s", bucket, firstKey),
		Chdir:   out,
	}
	res = rExtract.Run(ctx, extract)
	if res.ExitCode != ExitSuccess {
		t.Fatalf("extract exit=%d err=%v", res.ExitCode, res.Err)
	}

	for _, tc := range []struct {
		name string
		want string
	}{
		{name: "one.txt", want: "one"},
		{name: "two.txt", want: "two"},
	} {
		b, err := os.ReadFile(filepath.Join(out, tc.name))
		if err != nil {
			t.Fatalf("read %s: %v", tc.name, err)
		}
		if string(b) != tc.want {
			t.Fatalf("%s = %q, want %q", tc.name, string(b), tc.want)
		}
	}
}

// ---------------------------------------------------------------------------
// Test: create a compressed archive to S3 and extract it back
// ---------------------------------------------------------------------------
func TestS3CompressedRoundTrip(t *testing.T) {
	ctx := context.Background()
	ep := s3Endpoint(t)
	_, bucket := setupS3Bucket(t, ctx, ep)

	root := t.TempDir()
	srcDir := filepath.Join(root, "data")
	if err := os.MkdirAll(srcDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(srcDir, "msg.txt"), []byte("compressed-payload"), 0o644); err != nil {
		t.Fatal(err)
	}

	compressions := []struct {
		hint cli.CompressionHint
		ext  string
	}{
		{cli.CompressionGzip, ".tar.gz"},
		{cli.CompressionBzip2, ".tar.bz2"},
		{cli.CompressionXz, ".tar.xz"},
		{cli.CompressionZstd, ".tar.zst"},
		{cli.CompressionLz4, ".tar.lz4"},
	}

	for _, cc := range compressions {
		t.Run(string(cc.hint), func(t *testing.T) {
			archiveURI := fmt.Sprintf("s3://%s/archives/test%s", bucket, cc.ext)

			r := newRunnerWithEndpoint(t, ep, io.Discard, io.Discard)
			create := cli.Options{
				Mode:        cli.ModeCreate,
				Archive:     archiveURI,
				Compression: cc.hint,
				Chdir:       root,
				Members:     []string{"data"},
			}
			res := r.Run(ctx, create)
			if res.ExitCode != ExitSuccess {
				t.Fatalf("create exit=%d err=%v", res.ExitCode, res.Err)
			}

			outDir := t.TempDir()
			r2 := newRunnerWithEndpoint(t, ep, io.Discard, io.Discard)
			extract := cli.Options{Mode: cli.ModeExtract, Archive: archiveURI, Chdir: outDir}
			res = r2.Run(ctx, extract)
			if res.ExitCode != ExitSuccess {
				t.Fatalf("extract exit=%d err=%v", res.ExitCode, res.Err)
			}

			b, err := os.ReadFile(filepath.Join(outDir, "data", "msg.txt"))
			if err != nil {
				t.Fatalf("read extracted file: %v", err)
			}
			if string(b) != "compressed-payload" {
				t.Fatalf("content = %q", string(b))
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Test: create a zip archive to S3 and extract it back
// ---------------------------------------------------------------------------
func TestS3ZipRoundTrip(t *testing.T) {
	ctx := context.Background()
	ep := s3Endpoint(t)
	_, bucket := setupS3Bucket(t, ctx, ep)

	root := t.TempDir()
	srcDir := filepath.Join(root, "data")
	if err := os.MkdirAll(srcDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(srcDir, "msg.txt"), []byte("zip-payload"), 0o644); err != nil {
		t.Fatal(err)
	}

	archiveURI := fmt.Sprintf("s3://%s/archives/test.zip", bucket)
	r := newRunnerWithEndpoint(t, ep, io.Discard, io.Discard)
	create := cli.Options{
		Mode:    cli.ModeCreate,
		Archive: archiveURI,
		Chdir:   root,
		Members: []string{"data"},
	}
	res := r.Run(ctx, create)
	if res.ExitCode != ExitSuccess {
		t.Fatalf("create exit=%d err=%v", res.ExitCode, res.Err)
	}

	outDir := t.TempDir()
	r2 := newRunnerWithEndpoint(t, ep, io.Discard, io.Discard)
	extract := cli.Options{Mode: cli.ModeExtract, Archive: archiveURI, Chdir: outDir}
	res = r2.Run(ctx, extract)
	if res.ExitCode != ExitSuccess {
		t.Fatalf("extract exit=%d err=%v", res.ExitCode, res.Err)
	}

	b, err := os.ReadFile(filepath.Join(outDir, "data", "msg.txt"))
	if err != nil {
		t.Fatalf("read extracted file: %v", err)
	}
	if string(b) != "zip-payload" {
		t.Fatalf("content = %q", string(b))
	}
}

// ---------------------------------------------------------------------------
// Test: create a local zip archive from S3 members
// ---------------------------------------------------------------------------
func TestS3ZipMemberCreateLocalArchive(t *testing.T) {
	ctx := context.Background()
	ep := s3Endpoint(t)
	client, bucket := setupS3Bucket(t, ctx, ep)

	putObject(t, ctx, client, bucket, "docs/one.txt", "one-value")
	putObject(t, ctx, client, bucket, "docs/two.txt", "two-value")

	root := t.TempDir()
	archive := filepath.Join(root, "out.zip")

	r := newRunnerWithEndpoint(t, ep, io.Discard, io.Discard)
	create := cli.Options{
		Mode:    cli.ModeCreate,
		Archive: archive,
		Members: []string{
			fmt.Sprintf("s3://%s/docs/one.txt", bucket),
			fmt.Sprintf("s3://%s/docs/two.txt", bucket),
		},
	}
	res := r.Run(ctx, create)
	if res.ExitCode != ExitSuccess {
		t.Fatalf("create exit=%d err=%v", res.ExitCode, res.Err)
	}

	var listBuf bytes.Buffer
	r2 := newRunnerWithEndpoint(t, ep, &listBuf, io.Discard)
	list := cli.Options{Mode: cli.ModeList, Archive: archive}
	res = r2.Run(ctx, list)
	if res.ExitCode != ExitSuccess {
		t.Fatalf("list exit=%d err=%v", res.ExitCode, res.Err)
	}
	for _, want := range []string{"docs/one.txt", "docs/two.txt"} {
		if !strings.Contains(listBuf.String(), want) {
			t.Fatalf("listing missing %q:\n%s", want, listBuf.String())
		}
	}

	outDir := filepath.Join(root, "extracted")
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		t.Fatal(err)
	}
	r3 := newRunnerWithEndpoint(t, ep, io.Discard, io.Discard)
	extract := cli.Options{Mode: cli.ModeExtract, Archive: archive, Chdir: outDir}
	res = r3.Run(ctx, extract)
	if res.ExitCode != ExitSuccess {
		t.Fatalf("extract exit=%d err=%v", res.ExitCode, res.Err)
	}

	for _, tc := range []struct {
		file string
		want string
	}{
		{"docs/one.txt", "one-value"},
		{"docs/two.txt", "two-value"},
	} {
		b, err := os.ReadFile(filepath.Join(outDir, tc.file))
		if err != nil {
			t.Fatalf("read %s: %v", tc.file, err)
		}
		if string(b) != tc.want {
			t.Fatalf("%s content = %q, want %q", tc.file, string(b), tc.want)
		}
	}
}

// ---------------------------------------------------------------------------
// Test: full zip S3→S3 round trip (S3 members → S3 zip archive → extract to S3)
// ---------------------------------------------------------------------------
func TestS3ZipFullRoundTrip(t *testing.T) {
	ctx := context.Background()
	ep := s3Endpoint(t)
	client, bucket := setupS3Bucket(t, ctx, ep)

	putObject(t, ctx, client, bucket, "input/doc.txt", "document-content")
	putObject(t, ctx, client, bucket, "input/img.bin", "binary-content")

	archiveURI := fmt.Sprintf("s3://%s/roundtrip/archive.zip", bucket)

	r := newRunnerWithEndpoint(t, ep, io.Discard, io.Discard)
	create := cli.Options{
		Mode:    cli.ModeCreate,
		Archive: archiveURI,
		Members: []string{
			fmt.Sprintf("s3://%s/input/doc.txt", bucket),
			fmt.Sprintf("s3://%s/input/img.bin", bucket),
		},
	}
	res := r.Run(ctx, create)
	if res.ExitCode != ExitSuccess {
		t.Fatalf("create exit=%d err=%v", res.ExitCode, res.Err)
	}

	s3Target := fmt.Sprintf("s3://%s/output-zip/", bucket)
	r2 := newRunnerWithEndpoint(t, ep, io.Discard, io.Discard)
	extract := cli.Options{Mode: cli.ModeExtract, Archive: archiveURI, Chdir: s3Target}
	res = r2.Run(ctx, extract)
	if res.ExitCode != ExitSuccess {
		t.Fatalf("extract exit=%d err=%v", res.ExitCode, res.Err)
	}

	for _, tc := range []struct {
		key  string
		want string
	}{
		{"output-zip/input/doc.txt", "document-content"},
		{"output-zip/input/img.bin", "binary-content"},
	} {
		got := getObject(t, ctx, client, bucket, tc.key)
		if got != tc.want {
			t.Fatalf("s3://%s/%s = %q, want %q", bucket, tc.key, got, tc.want)
		}
	}
}

// ---------------------------------------------------------------------------
// Test: extracting zip symlink entries to S3 stores link target as object body
// ---------------------------------------------------------------------------
func TestS3ExtractZipSymlinkToS3(t *testing.T) {
	ctx := context.Background()
	ep := s3Endpoint(t)
	client, bucket := setupS3Bucket(t, ctx, ep)

	root := t.TempDir()
	archive := filepath.Join(root, "symlink.zip")
	payload := zipArchiveSymlinkBytes(t, "dir/link", "target.txt")
	if err := os.WriteFile(archive, payload, 0o644); err != nil {
		t.Fatalf("write zip: %v", err)
	}

	var stderr bytes.Buffer
	r := newRunnerWithEndpoint(t, ep, io.Discard, &stderr)
	extract := cli.Options{
		Mode:    cli.ModeExtract,
		Archive: archive,
		Chdir:   fmt.Sprintf("s3://%s/extracted/", bucket),
	}
	res := r.Run(ctx, extract)
	if res.ExitCode != ExitWarning {
		t.Fatalf("extract exit=%d err=%v, want warning", res.ExitCode, res.Err)
	}
	if !strings.Contains(stderr.String(), "zip symlink dir/link extracted to S3 as regular object") {
		t.Fatalf("stderr=%q, want symlink-to-s3 warning", stderr.String())
	}

	got := getObject(t, ctx, client, bucket, "extracted/dir/link")
	if got != "target.txt" {
		t.Fatalf("s3://%s/%s = %q, want %q", bucket, "extracted/dir/link", got, "target.txt")
	}
}

// ---------------------------------------------------------------------------
// Test: archive uploads to S3 set an explicit Content-Type
// ---------------------------------------------------------------------------
func TestS3ArchiveUploadSetsContentType(t *testing.T) {
	ctx := context.Background()
	ep := s3Endpoint(t)
	client, bucket := setupS3Bucket(t, ctx, ep)

	root := t.TempDir()
	srcDir := filepath.Join(root, "data")
	if err := os.MkdirAll(srcDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(srcDir, "msg.txt"), []byte("content-type"), 0o644); err != nil {
		t.Fatal(err)
	}

	archiveURI := fmt.Sprintf("s3://%s/archives/content-type.tar.gz", bucket)
	r := newRunnerWithEndpoint(t, ep, io.Discard, io.Discard)
	create := cli.Options{
		Mode:        cli.ModeCreate,
		Archive:     archiveURI,
		Compression: cli.CompressionGzip,
		Chdir:       root,
		Members:     []string{"data"},
	}
	res := r.Run(ctx, create)
	if res.ExitCode != ExitSuccess {
		t.Fatalf("create exit=%d err=%v", res.ExitCode, res.Err)
	}

	out, err := client.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: new(bucket),
		Key:    new("archives/content-type.tar.gz"),
	})
	if err != nil {
		t.Fatalf("get uploaded archive: %v", err)
	}
	defer out.Body.Close() // nolint: errcheck

	if aws.ToString(out.ContentType) != "application/gzip" {
		t.Fatalf("content-type=%q, want %q", aws.ToString(out.ContentType), "application/gzip")
	}
}

// ---------------------------------------------------------------------------
// Test: S3 archive reader propagates object Content-Type metadata
// ---------------------------------------------------------------------------
func TestS3ArchiveReaderReturnsContentType(t *testing.T) {
	ctx := context.Background()
	ep := s3Endpoint(t)
	client, bucket := setupS3Bucket(t, ctx, ep)

	key := "archives/noext"
	contentType := "application/gzip"
	_, err := client.PutObject(ctx, &awss3.PutObjectInput{
		Bucket:      new(bucket),
		Key:         new(key),
		Body:        strings.NewReader("payload"),
		ContentType: new(contentType),
	})
	if err != nil {
		t.Fatalf("put object: %v", err)
	}

	r := newRunnerWithEndpoint(t, ep, io.Discard, io.Discard)
	ref, err := locator.ParseArchive(fmt.Sprintf("s3://%s/%s", bucket, key))
	if err != nil {
		t.Fatalf("ParseArchive() error = %v", err)
	}
	rc, info, err := r.openArchiveReader(ctx, ref)
	if err != nil {
		t.Fatalf("openArchiveReader() error = %v", err)
	}
	defer rc.Close() //nolint:errcheck

	if _, err := io.Copy(io.Discard, rc); err != nil {
		t.Fatalf("read body: %v", err)
	}
	if info.ContentType != contentType {
		t.Fatalf("content-type=%q, want %q", info.ContentType, contentType)
	}
}

// ---------------------------------------------------------------------------
// Test: archive upload parses URI query string into S3 object metadata
// ---------------------------------------------------------------------------
func TestS3ArchiveUploadMetadataFromQuery(t *testing.T) {
	ctx := context.Background()
	ep := s3Endpoint(t)
	client, bucket := setupS3Bucket(t, ctx, ep)

	root := t.TempDir()
	srcDir := filepath.Join(root, "data")
	if err := os.MkdirAll(srcDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(srcDir, "msg.txt"), []byte("metadata-query"), 0o644); err != nil {
		t.Fatal(err)
	}

	archiveURI := fmt.Sprintf("s3://%s/archives/with-meta.tgz?key=value&team=platform", bucket)
	r := newRunnerWithEndpoint(t, ep, io.Discard, io.Discard)
	create := cli.Options{
		Mode:        cli.ModeCreate,
		Archive:     archiveURI,
		Compression: cli.CompressionGzip,
		Chdir:       root,
		Members:     []string{"data"},
	}
	res := r.Run(ctx, create)
	if res.ExitCode != ExitSuccess {
		t.Fatalf("create exit=%d err=%v", res.ExitCode, res.Err)
	}

	out, err := client.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: new(bucket),
		Key:    new("archives/with-meta.tgz"),
	})
	if err != nil {
		t.Fatalf("get uploaded archive: %v", err)
	}
	defer out.Body.Close() // nolint: errcheck

	if got := out.Metadata["key"]; got != "value" {
		t.Fatalf("metadata[key]=%q, want %q", got, "value")
	}
	if got := out.Metadata["team"]; got != "platform" {
		t.Fatalf("metadata[team]=%q, want %q", got, "platform")
	}
}

// ---------------------------------------------------------------------------
// Test: archive uploads to S3 apply Cache-Control from CLI flag
// ---------------------------------------------------------------------------
func TestS3ArchiveUploadCacheControlFromFlag(t *testing.T) {
	ctx := context.Background()
	ep := s3Endpoint(t)
	client, bucket := setupS3Bucket(t, ctx, ep)

	root := t.TempDir()
	srcDir := filepath.Join(root, "data")
	if err := os.MkdirAll(srcDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(srcDir, "msg.txt"), []byte("cache-control-flag"), 0o644); err != nil {
		t.Fatal(err)
	}

	archiveURI := fmt.Sprintf("s3://%s/archives/with-cache-control.tgz", bucket)
	r := newRunnerWithEndpoint(t, ep, io.Discard, io.Discard)
	create := cli.Options{
		Mode:           cli.ModeCreate,
		Archive:        archiveURI,
		Compression:    cli.CompressionGzip,
		Chdir:          root,
		S3CacheControl: "max-age=600,public",
		Members:        []string{"data"},
	}
	res := r.Run(ctx, create)
	if res.ExitCode != ExitSuccess {
		t.Fatalf("create exit=%d err=%v", res.ExitCode, res.Err)
	}

	out, err := client.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: new(bucket),
		Key:    new("archives/with-cache-control.tgz"),
	})
	if err != nil {
		t.Fatalf("get uploaded archive: %v", err)
	}
	defer out.Body.Close() // nolint: errcheck

	if got := aws.ToString(out.CacheControl); got != "max-age=600,public" {
		t.Fatalf("cache-control=%q, want %q", got, "max-age=600,public")
	}
}

// ---------------------------------------------------------------------------
// Test: archive uploads to S3 apply object tags, cache-control, and metadata
// together on the same object upload.
// ---------------------------------------------------------------------------
func TestS3ArchiveUploadObjectTagsFromFlag(t *testing.T) {
	ctx := context.Background()
	ep := s3Endpoint(t)
	client, bucket := setupS3Bucket(t, ctx, ep)

	root := t.TempDir()
	srcDir := filepath.Join(root, "data")
	if err := os.MkdirAll(srcDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(srcDir, "msg.txt"), []byte("tagged-archive"), 0o644); err != nil {
		t.Fatal(err)
	}

	archiveURI := fmt.Sprintf("s3://%s/archives/with-tags.tgz?env=prod&owner=platform", bucket)
	r := newRunnerWithEndpoint(t, ep, io.Discard, io.Discard)
	create := cli.Options{
		Mode:           cli.ModeCreate,
		Archive:        archiveURI,
		Compression:    cli.CompressionGzip,
		Chdir:          root,
		S3CacheControl: "max-age=300,public",
		S3ObjectTags: map[string]string{
			"team": "archive",
		},
		Members: []string{"data"},
	}
	res := r.Run(ctx, create)
	if res.ExitCode != ExitSuccess {
		t.Fatalf("create exit=%d err=%v", res.ExitCode, res.Err)
	}

	out, err := client.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: new(bucket),
		Key:    new("archives/with-tags.tgz"),
	})
	if err != nil {
		t.Fatalf("get uploaded archive: %v", err)
	}
	defer out.Body.Close() // nolint: errcheck

	if got := aws.ToString(out.CacheControl); got != "max-age=300,public" {
		t.Fatalf("cache-control=%q, want %q", got, "max-age=300,public")
	}
	if got := out.Metadata["env"]; got != "prod" {
		t.Fatalf("metadata[env]=%q, want %q", got, "prod")
	}
	if got := out.Metadata["owner"]; got != "platform" {
		t.Fatalf("metadata[owner]=%q, want %q", got, "platform")
	}

	tags := getObjectTags(t, ctx, client, bucket, "archives/with-tags.tgz")
	if tags["team"] != "archive" {
		t.Fatalf("tags[team]=%q, want %q", tags["team"], "archive")
	}
	if _, ok := tags["gotgz-created-at"]; ok {
		t.Fatalf("unexpected default gotgz-created-at tag in %#v", tags)
	}
}

// ---------------------------------------------------------------------------
// Test: extract a local archive to S3
// ---------------------------------------------------------------------------
func TestS3ExtractToS3(t *testing.T) {
	ctx := context.Background()
	ep := s3Endpoint(t)
	client, bucket := setupS3Bucket(t, ctx, ep)

	// Create a local archive first.
	root := t.TempDir()
	srcDir := filepath.Join(root, "src")
	if err := os.MkdirAll(srcDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(srcDir, "x.txt"), []byte("x-value"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(srcDir, "y.txt"), []byte("y-value"), 0o644); err != nil {
		t.Fatal(err)
	}

	localArchive := filepath.Join(root, "test.tar")
	r := newRunnerWithEndpoint(t, ep, io.Discard, io.Discard)
	create := cli.Options{
		Mode:    cli.ModeCreate,
		Archive: localArchive,
		Chdir:   root,
		Members: []string{"src"},
	}
	res := r.Run(ctx, create)
	if res.ExitCode != ExitSuccess {
		t.Fatalf("create exit=%d err=%v", res.ExitCode, res.Err)
	}

	// Extract into S3.
	s3Target := fmt.Sprintf("s3://%s/extracted/", bucket)
	r2 := newRunnerWithEndpoint(t, ep, io.Discard, io.Discard)
	extract := cli.Options{Mode: cli.ModeExtract, Archive: localArchive, Chdir: s3Target}
	res = r2.Run(ctx, extract)
	if res.ExitCode != ExitSuccess {
		t.Fatalf("extract-to-s3 exit=%d err=%v", res.ExitCode, res.Err)
	}

	// Verify objects exist in S3.
	for _, tc := range []struct {
		key, want string
	}{
		{"extracted/src/x.txt", "x-value"},
		{"extracted/src/y.txt", "y-value"},
	} {
		got := getObject(t, ctx, client, bucket, tc.key)
		if got != tc.want {
			t.Fatalf("s3://%s/%s = %q, want %q", bucket, tc.key, got, tc.want)
		}
	}
}

// ---------------------------------------------------------------------------
// Test: extracting a local tar archive to S3 applies Cache-Control from flag
// ---------------------------------------------------------------------------
func TestS3ExtractToS3CacheControlFromFlag(t *testing.T) {
	ctx := context.Background()
	ep := s3Endpoint(t)
	client, bucket := setupS3Bucket(t, ctx, ep)

	root := t.TempDir()
	srcDir := filepath.Join(root, "src")
	if err := os.MkdirAll(srcDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(srcDir, "x.txt"), []byte("x-value"), 0o644); err != nil {
		t.Fatal(err)
	}

	localArchive := filepath.Join(root, "test.tar")
	r := newRunnerWithEndpoint(t, ep, io.Discard, io.Discard)
	create := cli.Options{
		Mode:    cli.ModeCreate,
		Archive: localArchive,
		Chdir:   root,
		Members: []string{"src"},
	}
	res := r.Run(ctx, create)
	if res.ExitCode != ExitSuccess {
		t.Fatalf("create exit=%d err=%v", res.ExitCode, res.Err)
	}

	s3Target := fmt.Sprintf("s3://%s/extracted/", bucket)
	r2 := newRunnerWithEndpoint(t, ep, io.Discard, io.Discard)
	extract := cli.Options{
		Mode:           cli.ModeExtract,
		Archive:        localArchive,
		Chdir:          s3Target,
		S3CacheControl: "no-store",
	}
	res = r2.Run(ctx, extract)
	if res.ExitCode != ExitSuccess {
		t.Fatalf("extract-to-s3 exit=%d err=%v", res.ExitCode, res.Err)
	}

	out, err := client.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: new(bucket),
		Key:    new("extracted/src/x.txt"),
	})
	if err != nil {
		t.Fatalf("get extracted object: %v", err)
	}
	defer out.Body.Close() // nolint: errcheck

	if got := aws.ToString(out.CacheControl); got != "no-store" {
		t.Fatalf("cache-control=%q, want %q", got, "no-store")
	}
}

// ---------------------------------------------------------------------------
// Test: extracting a local tar archive to S3 applies object tags from flag
// ---------------------------------------------------------------------------
func TestS3ExtractToS3ObjectTagsFromFlag(t *testing.T) {
	ctx := context.Background()
	ep := s3Endpoint(t)
	client, bucket := setupS3Bucket(t, ctx, ep)

	root := t.TempDir()
	srcDir := filepath.Join(root, "src")
	if err := os.MkdirAll(srcDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(srcDir, "x.txt"), []byte("x-value"), 0o644); err != nil {
		t.Fatal(err)
	}

	localArchive := filepath.Join(root, "test.tar")
	r := newRunnerWithEndpoint(t, ep, io.Discard, io.Discard)
	create := cli.Options{
		Mode:    cli.ModeCreate,
		Archive: localArchive,
		Chdir:   root,
		Members: []string{"src"},
	}
	res := r.Run(ctx, create)
	if res.ExitCode != ExitSuccess {
		t.Fatalf("create exit=%d err=%v", res.ExitCode, res.Err)
	}

	s3Target := fmt.Sprintf("s3://%s/extracted/", bucket)
	r2 := newRunnerWithEndpoint(t, ep, io.Discard, io.Discard)
	extract := cli.Options{
		Mode:         cli.ModeExtract,
		Archive:      localArchive,
		Chdir:        s3Target,
		S3ObjectTags: map[string]string{"team": "restore"},
	}
	res = r2.Run(ctx, extract)
	if res.ExitCode != ExitSuccess {
		t.Fatalf("extract-to-s3 exit=%d err=%v", res.ExitCode, res.Err)
	}

	tags := getObjectTags(t, ctx, client, bucket, "extracted/src/x.txt")
	if tags["team"] != "restore" {
		t.Fatalf("tags[team]=%q, want %q", tags["team"], "restore")
	}
	if _, ok := tags["gotgz-created-at"]; ok {
		t.Fatalf("unexpected default gotgz-created-at tag in %#v", tags)
	}
}

// ---------------------------------------------------------------------------
// Test: extracting a local zip archive to S3 applies Cache-Control from flag
// ---------------------------------------------------------------------------
func TestS3ExtractZipToS3CacheControlFromFlag(t *testing.T) {
	ctx := context.Background()
	ep := s3Endpoint(t)
	client, bucket := setupS3Bucket(t, ctx, ep)

	root := t.TempDir()
	srcDir := filepath.Join(root, "src")
	if err := os.MkdirAll(srcDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(srcDir, "z.txt"), []byte("zip-value"), 0o644); err != nil {
		t.Fatal(err)
	}

	localArchive := filepath.Join(root, "test.zip")
	r := newRunnerWithEndpoint(t, ep, io.Discard, io.Discard)
	create := cli.Options{
		Mode:    cli.ModeCreate,
		Archive: localArchive,
		Chdir:   root,
		Members: []string{"src"},
	}
	res := r.Run(ctx, create)
	if res.ExitCode != ExitSuccess {
		t.Fatalf("create zip exit=%d err=%v", res.ExitCode, res.Err)
	}

	s3Target := fmt.Sprintf("s3://%s/extracted-zip/", bucket)
	r2 := newRunnerWithEndpoint(t, ep, io.Discard, io.Discard)
	extract := cli.Options{
		Mode:           cli.ModeExtract,
		Archive:        localArchive,
		Chdir:          s3Target,
		S3CacheControl: "max-age=120",
	}
	res = r2.Run(ctx, extract)
	if res.ExitCode != ExitSuccess {
		t.Fatalf("extract zip-to-s3 exit=%d err=%v", res.ExitCode, res.Err)
	}

	out, err := client.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: new(bucket),
		Key:    new("extracted-zip/src/z.txt"),
	})
	if err != nil {
		t.Fatalf("get extracted zip object: %v", err)
	}
	defer out.Body.Close() // nolint: errcheck

	if got := aws.ToString(out.CacheControl); got != "max-age=120" {
		t.Fatalf("cache-control=%q, want %q", got, "max-age=120")
	}
}

// ---------------------------------------------------------------------------
// Test: extracting a local zip archive to S3 applies object tags from flag
// ---------------------------------------------------------------------------
func TestS3ExtractZipToS3ObjectTagsFromFlag(t *testing.T) {
	ctx := context.Background()
	ep := s3Endpoint(t)
	client, bucket := setupS3Bucket(t, ctx, ep)

	root := t.TempDir()
	srcDir := filepath.Join(root, "src")
	if err := os.MkdirAll(srcDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(srcDir, "z.txt"), []byte("zip-value"), 0o644); err != nil {
		t.Fatal(err)
	}

	localArchive := filepath.Join(root, "test.zip")
	r := newRunnerWithEndpoint(t, ep, io.Discard, io.Discard)
	create := cli.Options{
		Mode:    cli.ModeCreate,
		Archive: localArchive,
		Chdir:   root,
		Members: []string{"src"},
	}
	res := r.Run(ctx, create)
	if res.ExitCode != ExitSuccess {
		t.Fatalf("create zip exit=%d err=%v", res.ExitCode, res.Err)
	}

	s3Target := fmt.Sprintf("s3://%s/extracted-zip/", bucket)
	r2 := newRunnerWithEndpoint(t, ep, io.Discard, io.Discard)
	extract := cli.Options{
		Mode:         cli.ModeExtract,
		Archive:      localArchive,
		Chdir:        s3Target,
		S3ObjectTags: map[string]string{"team": "zip-restore"},
	}
	res = r2.Run(ctx, extract)
	if res.ExitCode != ExitSuccess {
		t.Fatalf("extract zip-to-s3 exit=%d err=%v", res.ExitCode, res.Err)
	}

	tags := getObjectTags(t, ctx, client, bucket, "extracted-zip/src/z.txt")
	if tags["team"] != "zip-restore" {
		t.Fatalf("tags[team]=%q, want %q", tags["team"], "zip-restore")
	}
	if _, ok := tags["gotgz-created-at"]; ok {
		t.Fatalf("unexpected default gotgz-created-at tag in %#v", tags)
	}
}

// ---------------------------------------------------------------------------
// Test: list contents of an S3-hosted archive
// ---------------------------------------------------------------------------
func TestS3ListArchive(t *testing.T) {
	ctx := context.Background()
	ep := s3Endpoint(t)
	_, bucket := setupS3Bucket(t, ctx, ep)

	// Build a local archive, upload to S3, then list via S3 URI.
	root := t.TempDir()
	srcDir := filepath.Join(root, "files")
	if err := os.MkdirAll(srcDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(srcDir, "one.txt"), []byte("1"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(srcDir, "two.txt"), []byte("2"), 0o644); err != nil {
		t.Fatal(err)
	}

	archiveURI := fmt.Sprintf("s3://%s/list-test.tar", bucket)
	r := newRunnerWithEndpoint(t, ep, io.Discard, io.Discard)
	create := cli.Options{
		Mode:    cli.ModeCreate,
		Archive: archiveURI,
		Chdir:   root,
		Members: []string{"files"},
	}
	res := r.Run(ctx, create)
	if res.ExitCode != ExitSuccess {
		t.Fatalf("create exit=%d err=%v", res.ExitCode, res.Err)
	}

	var listBuf bytes.Buffer
	r2 := newRunnerWithEndpoint(t, ep, &listBuf, io.Discard)
	list := cli.Options{Mode: cli.ModeList, Archive: archiveURI}
	res = r2.Run(ctx, list)
	if res.ExitCode != ExitSuccess {
		t.Fatalf("list exit=%d err=%v", res.ExitCode, res.Err)
	}

	output := listBuf.String()
	for _, want := range []string{"files/", "files/one.txt", "files/two.txt"} {
		if !strings.Contains(output, want) {
			t.Errorf("listing missing %q:\n%s", want, output)
		}
	}
}

// ---------------------------------------------------------------------------
// Test: full S3→S3 round trip (S3 members → S3 archive → extract to S3)
// ---------------------------------------------------------------------------
func TestS3FullRoundTrip(t *testing.T) {
	ctx := context.Background()
	ep := s3Endpoint(t)
	client, bucket := setupS3Bucket(t, ctx, ep)

	// Seed some S3 objects.
	putObject(t, ctx, client, bucket, "input/doc.txt", "document-content")
	putObject(t, ctx, client, bucket, "input/img.bin", "binary-content")

	archiveURI := fmt.Sprintf("s3://%s/roundtrip/archive.tar.gz", bucket)

	// Create: S3 members → S3 archive (gzip).
	r := newRunnerWithEndpoint(t, ep, io.Discard, io.Discard)
	create := cli.Options{
		Mode:        cli.ModeCreate,
		Archive:     archiveURI,
		Compression: cli.CompressionGzip,
		Members: []string{
			fmt.Sprintf("s3://%s/input/doc.txt", bucket),
			fmt.Sprintf("s3://%s/input/img.bin", bucket),
		},
	}
	res := r.Run(ctx, create)
	if res.ExitCode != ExitSuccess {
		t.Fatalf("create exit=%d err=%v", res.ExitCode, res.Err)
	}

	// Extract: S3 archive → S3 destination.
	s3Target := fmt.Sprintf("s3://%s/output/", bucket)
	r2 := newRunnerWithEndpoint(t, ep, io.Discard, io.Discard)
	extract := cli.Options{Mode: cli.ModeExtract, Archive: archiveURI, Chdir: s3Target}
	res = r2.Run(ctx, extract)
	if res.ExitCode != ExitSuccess {
		t.Fatalf("extract exit=%d err=%v", res.ExitCode, res.Err)
	}

	for _, tc := range []struct {
		key, want string
	}{
		{"output/input/doc.txt", "document-content"},
		{"output/input/img.bin", "binary-content"},
	} {
		got := getObject(t, ctx, client, bucket, tc.key)
		if got != tc.want {
			t.Fatalf("s3://%s/%s = %q, want %q", bucket, tc.key, got, tc.want)
		}
	}
}

// ---------------------------------------------------------------------------
// Test: verbose output works with S3 operations
// ---------------------------------------------------------------------------
func TestS3VerboseOutput(t *testing.T) {
	ctx := context.Background()
	ep := s3Endpoint(t)
	client, bucket := setupS3Bucket(t, ctx, ep)

	putObject(t, ctx, client, bucket, "v/note.txt", "verbose-test")

	root := t.TempDir()
	archive := filepath.Join(root, "verbose.tar")

	var createBuf bytes.Buffer
	r := newRunnerWithEndpoint(t, ep, &createBuf, io.Discard)
	create := cli.Options{
		Mode:    cli.ModeCreate,
		Archive: archive,
		Verbose: true,
		Members: []string{fmt.Sprintf("s3://%s/v/note.txt", bucket)},
	}
	res := r.Run(ctx, create)
	if res.ExitCode != ExitSuccess {
		t.Fatalf("create exit=%d err=%v", res.ExitCode, res.Err)
	}
	if !strings.Contains(createBuf.String(), "v/note.txt") {
		t.Fatalf("verbose create output missing file name:\n%s", createBuf.String())
	}

	// Verbose extract.
	outDir := filepath.Join(root, "out")
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		t.Fatal(err)
	}
	var extractBuf bytes.Buffer
	r2 := newRunnerWithEndpoint(t, ep, &extractBuf, io.Discard)
	extract := cli.Options{Mode: cli.ModeExtract, Archive: archive, Chdir: outDir, Verbose: true}
	res = r2.Run(ctx, extract)
	if res.ExitCode != ExitSuccess {
		t.Fatalf("extract exit=%d err=%v", res.ExitCode, res.Err)
	}
	if !strings.Contains(extractBuf.String(), "v/note.txt") {
		t.Fatalf("verbose extract output missing file name:\n%s", extractBuf.String())
	}
}

// ---------------------------------------------------------------------------
// Test: exclude patterns work with S3 members
// ---------------------------------------------------------------------------
func TestS3ExcludePattern(t *testing.T) {
	ctx := context.Background()
	ep := s3Endpoint(t)
	client, bucket := setupS3Bucket(t, ctx, ep)

	putObject(t, ctx, client, bucket, "mix/keep.txt", "keep-me")
	putObject(t, ctx, client, bucket, "mix/skip.log", "skip-me")

	root := t.TempDir()
	archive := filepath.Join(root, "exclude.tar")

	r := newRunnerWithEndpoint(t, ep, io.Discard, io.Discard)
	create := cli.Options{
		Mode:    cli.ModeCreate,
		Archive: archive,
		Exclude: []string{"mix/skip.log"},
		Members: []string{
			fmt.Sprintf("s3://%s/mix/keep.txt", bucket),
			fmt.Sprintf("s3://%s/mix/skip.log", bucket),
		},
	}
	res := r.Run(ctx, create)
	if res.ExitCode != ExitSuccess {
		t.Fatalf("create exit=%d err=%v", res.ExitCode, res.Err)
	}

	var listBuf bytes.Buffer
	r2 := newRunnerWithEndpoint(t, ep, &listBuf, io.Discard)
	list := cli.Options{Mode: cli.ModeList, Archive: archive}
	res = r2.Run(ctx, list)
	if res.ExitCode != ExitSuccess {
		t.Fatalf("list exit=%d err=%v", res.ExitCode, res.Err)
	}

	output := listBuf.String()
	if !strings.Contains(output, "mix/keep.txt") {
		t.Fatalf("listing should contain keep.txt:\n%s", output)
	}
	if strings.Contains(output, "mix/skip.log") {
		t.Fatalf("listing should NOT contain skip.log:\n%s", output)
	}
}

// ---------------------------------------------------------------------------
// Test: extract to stdout (-O) from S3 archive
// ---------------------------------------------------------------------------
func TestS3ExtractToStdout(t *testing.T) {
	ctx := context.Background()
	ep := s3Endpoint(t)
	_, bucket := setupS3Bucket(t, ctx, ep)

	// Create a local archive with a file, upload to S3, then extract with -O.
	root := t.TempDir()
	srcDir := filepath.Join(root, "src")
	if err := os.MkdirAll(srcDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(srcDir, "out.txt"), []byte("stdout-test"), 0o644); err != nil {
		t.Fatal(err)
	}

	archiveURI := fmt.Sprintf("s3://%s/stdout-test.tar", bucket)
	r := newRunnerWithEndpoint(t, ep, io.Discard, io.Discard)
	create := cli.Options{
		Mode:    cli.ModeCreate,
		Archive: archiveURI,
		Chdir:   root,
		Members: []string{"src"},
	}
	res := r.Run(ctx, create)
	if res.ExitCode != ExitSuccess {
		t.Fatalf("create exit=%d err=%v", res.ExitCode, res.Err)
	}

	var stdoutBuf bytes.Buffer
	r2 := newRunnerWithEndpoint(t, ep, &stdoutBuf, io.Discard)
	extract := cli.Options{
		Mode:     cli.ModeExtract,
		Archive:  archiveURI,
		ToStdout: true,
		Members:  []string{"src/out.txt"},
	}
	res = r2.Run(ctx, extract)
	if res.ExitCode != ExitSuccess {
		t.Fatalf("extract exit=%d err=%v", res.ExitCode, res.Err)
	}
	if !strings.Contains(stdoutBuf.String(), "stdout-test") {
		t.Fatalf("stdout output = %q, want to contain 'stdout-test'", stdoutBuf.String())
	}
}
