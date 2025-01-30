package gotgz

import (
	"context"
	"math/rand/v2"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"syscall"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Use this command to run test locally:
// S_CI=true go test -v -run ^TestS3$ .

func TestS3(t *testing.T) {
	if os.Getenv("IS_CI") != "true" {
		t.Skip("skipping test; IS_CI is not set")
		return
	}

	bucketName := "test-" + strconv.FormatUint(rand.Uint64(), 10)

	// set env AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("AWS_ACCESS_KEY_ID", "foo")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "bar")
	t.Setenv("AWS_ENDPOINT_URL", "http://127.0.0.1:4566")

	basectx := context.Background()
	client, err := New(basectx, bucketName)
	if err != nil {
		t.Fatal(err)
	}

	// create bucket
	_, err = client.s3Client.CreateBucket(basectx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatal(err)
	}

	gzip := GZipArchiver{Level: 9}
	metadata := map[string]string{"x-client": "gotgz"}
	fileName := "testdata.tar.gz"

	createFlags := CompressFlags{Archiver: gzip}
	err = client.Upload(basectx, fileName, metadata, createFlags, "testdata")
	if err != nil {
		t.Fatal(err)
	}

	exist, err := client.IsExist(basectx, fileName)
	if err != nil {
		t.Fatal(err)
	}
	if !exist {
		t.Errorf("file %s not exist", fileName)
	}

	temp := t.TempDir()
	extractFlags := DecompressFlags{Archiver: gzip}
	metadata2, err := client.Download(basectx, fileName, temp, extractFlags)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(metadata, metadata2) {
		t.Errorf("metadata not equal: %v, %v", metadata, metadata2)
	}

	{
		origin := make(map[string]TestFileInfo)
		err := filepath.Walk("testdata", func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			var hash string
			if info.Mode().IsRegular() {
				hash, err = GetFileHash(path)
				if err != nil {
					return err
				}
			}

			var file = TestFileInfo{
				Hash: hash,
				Mode: uint32(info.Mode()),
			}

			if sys, ok := info.Sys().(*syscall.Stat_t); ok {
				file.Uid = int(sys.Uid)
				file.Gid = int(sys.Gid)
			}

			if IsSymbolicLink(info.Mode()) {
				file.Link, err = os.Readlink(path)
				if err != nil {
					return err
				}
			}
			origin[path] = file
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}

		processed := make(map[string]TestFileInfo)
		err = filepath.Walk(temp, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			if temp == path {
				return nil
			}

			var hash string
			if info.Mode().IsRegular() {
				hash, err = GetFileHash(path)
				if err != nil {
					return err
				}
			}

			var file = TestFileInfo{
				Hash: hash,
				Mode: uint32(info.Mode()),
			}

			rel, err := filepath.Rel(temp, path)
			if err != nil {
				return err
			}

			if sys, ok := info.Sys().(*syscall.Stat_t); ok {
				file.Uid = int(sys.Uid)
				file.Gid = int(sys.Gid)
			}

			if IsSymbolicLink(info.Mode()) {
				file.Link, err = os.Readlink(path)
				if err != nil {
					return err
				}
			}

			processed[rel] = file
			return nil
		})

		if err != nil {
			t.Fatal(err)
		}

		if len(origin) != len(processed) {
			t.Fatalf("origin: %d, processed: %d", len(origin), len(processed))
		}

		for rel, originFile := range origin {
			processedFile, ok := processed[rel]
			if !ok {
				t.Fatalf("file %s not found", rel)
			}

			if originFile.Hash != processedFile.Hash {
				t.Fatalf("file %s hash not match", rel)
			}

			if originFile.Mode != processedFile.Mode {
				t.Fatalf("file %s mode not match", rel)
			}

			if originFile.Uid != processedFile.Uid {
				t.Fatalf("file %s uid not match", rel)
			}

			if originFile.Gid != processedFile.Gid {
				t.Fatalf("file %s gid not match", rel)
			}
		}
	}
}
