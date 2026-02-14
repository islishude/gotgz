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
	"github.com/islishude/gotgz/internal/cli"
)

// s3Endpoint returns the LocalStack endpoint if configured, or skips the test.
func s3Endpoint(t *testing.T) string {
	t.Helper()
	ep := os.Getenv("GOTGZ_TEST_S3_ENDPOINT")
	if ep == "" {
		t.Skip("GOTGZ_TEST_S3_ENDPOINT not set; skipping S3 integration test")
	}
	return ep
}

// setupS3Bucket creates a temporary S3 bucket on LocalStack and returns its name.
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
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})

	bucket := fmt.Sprintf("gotgz-test-%d", os.Getpid())
	_, err = client.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(bucket)})
	if err != nil {
		t.Fatalf("create bucket %s: %v", bucket, err)
	}

	t.Cleanup(func() {
		// Delete all objects then the bucket.
		list, _ := client.ListObjectsV2(ctx, &awss3.ListObjectsV2Input{Bucket: aws.String(bucket)})
		if list != nil {
			for _, obj := range list.Contents {
				_, _ = client.DeleteObject(ctx, &awss3.DeleteObjectInput{Bucket: aws.String(bucket), Key: obj.Key})
			}
		}
		_, _ = client.DeleteBucket(ctx, &awss3.DeleteBucketInput{Bucket: aws.String(bucket)})
	})
	return client, bucket
}

// putObject is a small helper to upload an object to LocalStack.
func putObject(t *testing.T, ctx context.Context, client *awss3.Client, bucket, key, body string) {
	t.Helper()
	_, err := client.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
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
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
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

// newRunnerWithEndpoint creates a Runner pointing at the given S3 endpoint.
func newRunnerWithEndpoint(t *testing.T, endpoint string, stdout, stderr io.Writer) *Runner {
	t.Helper()
	// Point the AWS SDK at LocalStack via environment.
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
