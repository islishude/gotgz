package locator

import (
	"reflect"
	"strings"
	"testing"
)

// TestParseExtractTarget verifies target parsing defaults and S3 option wiring.
func TestParseExtractTarget(t *testing.T) {
	t.Run("empty chdir defaults to current directory", func(t *testing.T) {
		got, err := ParseExtractTarget("", "", nil)
		if err != nil {
			t.Fatalf("ParseExtractTarget() error = %v", err)
		}
		want := Ref{Kind: KindLocal, Raw: ".", Path: "."}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("ParseExtractTarget() = %+v, want %+v", got, want)
		}
	})

	t.Run("s3 target applies cache control and object tags", func(t *testing.T) {
		tags := map[string]string{"team": "archive"}
		got, err := ParseExtractTarget("s3://bucket/prefix", " max-age=60 ", tags)
		if err != nil {
			t.Fatalf("ParseExtractTarget() error = %v", err)
		}
		if got.Kind != KindS3 || got.Bucket != "bucket" || got.Key != "prefix" {
			t.Fatalf("ParseExtractTarget() = %+v, want S3 bucket/key", got)
		}
		if got.CacheControl != "max-age=60" {
			t.Fatalf("cache-control = %q, want %q", got.CacheControl, "max-age=60")
		}
		if !reflect.DeepEqual(got.ObjectTags, map[string]string{"team": "archive"}) {
			t.Fatalf("object tags = %#v, want %#v", got.ObjectTags, map[string]string{"team": "archive"})
		}

		tags["team"] = "changed"
		if got.ObjectTags["team"] != "archive" {
			t.Fatalf("object tags should be cloned, got %#v", got.ObjectTags)
		}
	})

	t.Run("local target ignores s3 options", func(t *testing.T) {
		got, err := ParseExtractTarget("./out", "no-store", map[string]string{"team": "archive"})
		if err != nil {
			t.Fatalf("ParseExtractTarget() error = %v", err)
		}
		if got.Kind != KindLocal || got.Path != "./out" {
			t.Fatalf("ParseExtractTarget() = %+v, want local path", got)
		}
		if got.CacheControl != "" || got.ObjectTags != nil {
			t.Fatalf("local target should not include S3 options, got %+v", got)
		}
	})
}

// TestRefWithS3CacheControl verifies cache-control normalization and S3 gating.
func TestRefWithS3CacheControl(t *testing.T) {
	t.Run("s3 value is trimmed and applied", func(t *testing.T) {
		ref := Ref{Kind: KindS3, Bucket: "bucket", Key: "out.tar"}
		got := ref.WithS3CacheControl(" no-store ")
		if got.CacheControl != "no-store" {
			t.Fatalf("cache-control = %q, want %q", got.CacheControl, "no-store")
		}
	})

	t.Run("blank value is ignored", func(t *testing.T) {
		ref := Ref{Kind: KindS3, Bucket: "bucket", Key: "out.tar", CacheControl: "existing"}
		got := ref.WithS3CacheControl("   ")
		if got.CacheControl != "existing" {
			t.Fatalf("cache-control = %q, want %q", got.CacheControl, "existing")
		}
	})

	t.Run("non s3 ref is unchanged", func(t *testing.T) {
		ref := Ref{Kind: KindLocal, Path: "out.tar"}
		got := ref.WithS3CacheControl("no-store")
		if !reflect.DeepEqual(got, ref) {
			t.Fatalf("WithS3CacheControl() = %+v, want %+v", got, ref)
		}
	})
}

// TestRefWithS3ObjectTags verifies S3 tag application and map cloning semantics.
func TestRefWithS3ObjectTags(t *testing.T) {
	t.Run("s3 tags are cloned", func(t *testing.T) {
		ref := Ref{Kind: KindS3, Bucket: "bucket", Key: "out.tar"}
		tags := map[string]string{"team": "archive"}
		got := ref.WithS3ObjectTags(tags)
		if !reflect.DeepEqual(got.ObjectTags, map[string]string{"team": "archive"}) {
			t.Fatalf("object tags = %#v, want %#v", got.ObjectTags, map[string]string{"team": "archive"})
		}
		tags["team"] = "changed"
		if got.ObjectTags["team"] != "archive" {
			t.Fatalf("object tags should be cloned, got %#v", got.ObjectTags)
		}
	})

	t.Run("empty tags are ignored", func(t *testing.T) {
		ref := Ref{Kind: KindS3, Bucket: "bucket", Key: "out.tar", ObjectTags: map[string]string{"owner": "ops"}}
		got := ref.WithS3ObjectTags(nil)
		if !reflect.DeepEqual(got.ObjectTags, map[string]string{"owner": "ops"}) {
			t.Fatalf("object tags = %#v, want %#v", got.ObjectTags, map[string]string{"owner": "ops"})
		}
	})

	t.Run("non s3 ref is unchanged", func(t *testing.T) {
		ref := Ref{Kind: KindLocal, Path: "out.tar"}
		got := ref.WithS3ObjectTags(map[string]string{"team": "archive"})
		if !reflect.DeepEqual(got, ref) {
			t.Fatalf("WithS3ObjectTags() = %+v, want %+v", got, ref)
		}
	})
}

// TestRefWithArchiveSuffix verifies suffix rewrites for local and S3 targets.
func TestRefWithArchiveSuffix(t *testing.T) {
	t.Run("local path and raw are rewritten", func(t *testing.T) {
		ref := Ref{Kind: KindLocal, Path: "/tmp/out.tar.gz", Raw: "/tmp/out.tar.gz"}
		got, err := ref.WithArchiveSuffix("daily")
		if err != nil {
			t.Fatalf("WithArchiveSuffix() error = %v", err)
		}
		if got.Path != "/tmp/out-daily.tar.gz" || got.Raw != "/tmp/out-daily.tar.gz" {
			t.Fatalf("WithArchiveSuffix() = %+v, want rewritten local path/raw", got)
		}
	})

	t.Run("s3 key is rewritten", func(t *testing.T) {
		ref := Ref{Kind: KindS3, Bucket: "bucket", Key: "out.tar.gz"}
		got, err := ref.WithArchiveSuffix("daily")
		if err != nil {
			t.Fatalf("WithArchiveSuffix() error = %v", err)
		}
		if got.Key != "out-daily.tar.gz" {
			t.Fatalf("key = %q, want %q", got.Key, "out-daily.tar.gz")
		}
	})

	t.Run("stdio target is rejected", func(t *testing.T) {
		ref := Ref{Kind: KindStdio, Raw: "-"}
		_, err := ref.WithArchiveSuffix("daily")
		if err == nil || !strings.Contains(err.Error(), "cannot use -suffix") {
			t.Fatalf("WithArchiveSuffix() err = %v, want stdio error", err)
		}
	})
}
