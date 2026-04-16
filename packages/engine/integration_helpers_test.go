//go:build integration

package engine

import (
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/islishude/gotgz/packages/cli"
)

func integrationS3Endpoint(t *testing.T) string {
	t.Helper()
	ep := os.Getenv("GOTGZ_TEST_S3_ENDPOINT")
	if ep == "" {
		t.Skip("GOTGZ_TEST_S3_ENDPOINT not set")
	}
	return ep
}

func integrationS3Client(t *testing.T, ctx context.Context, endpoint string) *awss3.Client {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: "test", SecretAccessKey: "test"}, nil
		})),
	)
	if err != nil {
		t.Fatalf("LoadDefaultConfig() error = %v", err)
	}
	return awss3.NewFromConfig(cfg, func(o *awss3.Options) {
		o.BaseEndpoint = new(endpoint)
		o.UsePathStyle = true
	})
}

func setupS3Bucket(t *testing.T, ctx context.Context, endpoint string) (*awss3.Client, string) {
	t.Helper()
	client := integrationS3Client(t, ctx, endpoint)
	name := strings.ToLower(strings.ReplaceAll(t.Name(), "/", "-"))
	bucket := fmt.Sprintf("gotgz-%s-%d", name, rand.Int32N(1000000))
	if len(bucket) > 63 {
		bucket = bucket[:63]
	}
	// Bucket names cannot end with a hyphen, but our test names often do.
	// Replace a trailing hyphen with 'x' if needed.
	if strings.HasSuffix(bucket, "-") {
		bucket = bucket[:len(bucket)-1] + "x"
	}
	_, err := client.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: new(bucket)})
	if err != nil {
		t.Fatalf("CreateBucket(%s) error = %v", bucket, err)
	}
	t.Cleanup(func() {
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

func compressionForArchive(name string) cli.CompressionHint {
	switch {
	case strings.HasSuffix(name, ".tar.gz"):
		return cli.CompressionGzip
	case strings.HasSuffix(name, ".tar.zst"):
		return cli.CompressionZstd
	case strings.HasSuffix(name, ".tar"):
		return cli.CompressionNone
	default:
		return cli.CompressionAuto
	}
}

func putObject(t *testing.T, ctx context.Context, client *awss3.Client, bucket, key, body string) {
	t.Helper()
	_, err := client.PutObject(ctx, &awss3.PutObjectInput{Bucket: new(bucket), Key: new(key), Body: strings.NewReader(body)})
	if err != nil {
		t.Fatalf("PutObject(%s/%s) error = %v", bucket, key, err)
	}
}

func getObject(t *testing.T, ctx context.Context, client *awss3.Client, bucket, key string) string {
	t.Helper()
	out, err := client.GetObject(ctx, &awss3.GetObjectInput{Bucket: new(bucket), Key: new(key)})
	if err != nil {
		t.Fatalf("GetObject(%s/%s) error = %v", bucket, key, err)
	}
	defer out.Body.Close() //nolint:errcheck
	b, err := io.ReadAll(out.Body)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	return string(b)
}

func getObjectTags(t *testing.T, ctx context.Context, client *awss3.Client, bucket, key string) map[string]string {
	t.Helper()
	out, err := client.GetObjectTagging(ctx, &awss3.GetObjectTaggingInput{Bucket: new(bucket), Key: new(key)})
	if err != nil {
		t.Fatalf("GetObjectTagging(%s/%s) error = %v", bucket, key, err)
	}
	tags := make(map[string]string, len(out.TagSet))
	for _, tag := range out.TagSet {
		tags[aws.ToString(tag.Key)] = aws.ToString(tag.Value)
	}
	return tags
}

func headObject(t *testing.T, ctx context.Context, client *awss3.Client, bucket, key string) *awss3.HeadObjectOutput {
	t.Helper()
	out, err := client.HeadObject(ctx, &awss3.HeadObjectInput{Bucket: new(bucket), Key: new(key)})
	if err != nil {
		t.Fatalf("HeadObject(%s/%s) error = %v", bucket, key, err)
	}
	return out
}

func newRunnerWithEndpoint(t *testing.T, endpoint string, stdout, stderr io.Writer) *Runner {
	t.Helper()
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
