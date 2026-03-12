package engine

import (
	"archive/tar"
	"context"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/islishude/gotgz/packages/archive"
	"github.com/islishude/gotgz/packages/archivepath"
	"github.com/islishude/gotgz/packages/cli"
	"github.com/islishude/gotgz/packages/locator"
	localstore "github.com/islishude/gotgz/packages/storage/local"
)

type fakeLocalArchiveStore struct {
	openReader func(ref locator.Ref) (io.ReadCloser, localstore.Metadata, error)
	openWriter func(ref locator.Ref) (io.WriteCloser, error)
}

func (f fakeLocalArchiveStore) OpenReader(ref locator.Ref) (io.ReadCloser, localstore.Metadata, error) {
	if f.openReader == nil {
		return nil, localstore.Metadata{}, nil
	}
	return f.openReader(ref)
}

func (f fakeLocalArchiveStore) OpenWriter(ref locator.Ref) (io.WriteCloser, error) {
	if f.openWriter == nil {
		return nil, nil
	}
	return f.openWriter(ref)
}

func TestResolvePolicyOverrides(t *testing.T) {
	so := false
	sp := true
	p := resolvePolicy(cli.Options{SameOwner: &so, SamePermissions: &sp, NumericOwner: true})
	if p.SameOwner {
		t.Fatalf("SameOwner should be false")
	}
	if !p.SamePerms {
		t.Fatalf("SamePerms should be true")
	}
	if !p.NumericOwner {
		t.Fatalf("NumericOwner should be true")
	}
}

func TestApplyS3CacheControl(t *testing.T) {
	ref := locator.Ref{Kind: locator.KindS3, Bucket: "bucket", Key: "out.tar"}
	got := ref.WithS3CacheControl(" max-age=3600 ")
	if got.CacheControl != "max-age=3600" {
		t.Fatalf("cache-control = %q, want %q", got.CacheControl, "max-age=3600")
	}
}

func TestApplyS3CacheControlNonS3Ignored(t *testing.T) {
	ref := locator.Ref{Kind: locator.KindLocal, Path: "/tmp/out.tar"}
	got := ref.WithS3CacheControl("no-store")
	if got.CacheControl != "" {
		t.Fatalf("non-s3 ref should ignore cache-control, got %q", got.CacheControl)
	}
}

func TestApplyS3ObjectTags(t *testing.T) {
	ref := locator.Ref{Kind: locator.KindS3, Bucket: "bucket", Key: "out.tar"}
	tags := map[string]string{"team": "archive"}
	got := ref.WithS3ObjectTags(tags)
	if !reflect.DeepEqual(got.ObjectTags, tags) {
		t.Fatalf("object tags = %#v, want %#v", got.ObjectTags, tags)
	}

	tags["team"] = "changed"
	if got.ObjectTags["team"] != "archive" {
		t.Fatalf("object tags should be cloned, got %#v", got.ObjectTags)
	}
}

func TestApplyS3ObjectTagsNonS3Ignored(t *testing.T) {
	ref := locator.Ref{Kind: locator.KindLocal, Path: "/tmp/out.tar"}
	got := ref.WithS3ObjectTags(map[string]string{"team": "archive"})
	if got.ObjectTags != nil {
		t.Fatalf("non-s3 ref should ignore object tags, got %#v", got.ObjectTags)
	}
}

func TestApplyArchiveSuffix(t *testing.T) {
	t.Run("local", func(t *testing.T) {
		got, err := (locator.Ref{Kind: locator.KindLocal, Path: "/tmp/out.tar.gz", Raw: "/tmp/out.tar.gz"}).WithArchiveSuffix("daily")
		if err != nil {
			t.Fatalf("WithArchiveSuffix() error = %v", err)
		}
		if got.Path != "/tmp/out-daily.tar.gz" || got.Raw != got.Path {
			t.Fatalf("local suffix result = %+v", got)
		}
	})

	t.Run("s3", func(t *testing.T) {
		got, err := (locator.Ref{Kind: locator.KindS3, Bucket: "bucket", Key: "out.tar.gz"}).WithArchiveSuffix("daily")
		if err != nil {
			t.Fatalf("WithArchiveSuffix() error = %v", err)
		}
		if got.Key != "out-daily.tar.gz" {
			t.Fatalf("s3 key = %q, want %q", got.Key, "out-daily.tar.gz")
		}
	})

	t.Run("stdio rejected", func(t *testing.T) {
		_, err := (locator.Ref{Kind: locator.KindStdio, Raw: "-"}).WithArchiveSuffix("daily")
		if err == nil || !strings.Contains(err.Error(), "cannot use -suffix") {
			t.Fatalf("WithArchiveSuffix() err = %v, want stdio error", err)
		}
	})
}

func TestParseExtractTarget(t *testing.T) {
	got, err := locator.ParseExtractTarget("s3://bucket/prefix", " max-age=60 ", map[string]string{"team": "archive"})
	if err != nil {
		t.Fatalf("ParseExtractTarget() error = %v", err)
	}
	if got.Kind != locator.KindS3 || got.Bucket != "bucket" || got.Key != "prefix" {
		t.Fatalf("parsed target = %+v", got)
	}
	if got.CacheControl != "max-age=60" {
		t.Fatalf("cache-control = %q, want %q", got.CacheControl, "max-age=60")
	}
	if !reflect.DeepEqual(got.ObjectTags, map[string]string{"team": "archive"}) {
		t.Fatalf("object tags = %#v", got.ObjectTags)
	}
}

func TestProcessCreateMembers(t *testing.T) {
	ctx := context.Background()
	opts := cli.Options{Members: []string{"src/file.txt", "s3://bucket/object.txt", "s3://bucket/skip.txt"}}
	var seen []string

	warnings, err := (&Runner{}).processCreateMembers(
		ctx,
		opts,
		archivepath.NewCompiledPathMatcher([]string{"skip.txt"}),
		func(ref locator.Ref) error {
			seen = append(seen, "s3:"+ref.Key)
			return nil
		},
		func(member string) (int, error) {
			seen = append(seen, "local:"+member)
			return 2, nil
		},
	)
	if err != nil {
		t.Fatalf("processCreateMembers() error = %v", err)
	}
	if warnings != 2 {
		t.Fatalf("warnings = %d, want 2", warnings)
	}
	if strings.Join(seen, ",") != "local:src/file.txt,s3:object.txt" {
		t.Fatalf("seen = %v", seen)
	}
}

func TestDispatchExtractTarget(t *testing.T) {
	r := &Runner{}

	t.Run("local", func(t *testing.T) {
		warnings, err := r.dispatchExtractTarget(
			locator.Ref{Kind: locator.KindLocal, Path: "/tmp/out"},
			"/tmp/out",
			func(target locator.Ref) (int, error) {
				t.Fatalf("unexpected s3 dispatch: %+v", target)
				return 0, nil
			},
			func(base string) (int, error) {
				if base != "/tmp/out" {
					t.Fatalf("base = %q", base)
				}
				return 3, nil
			},
		)
		if err != nil || warnings != 3 {
			t.Fatalf("dispatchExtractTarget() = (%d, %v), want (3, nil)", warnings, err)
		}
	})

	t.Run("s3", func(t *testing.T) {
		warnings, err := r.dispatchExtractTarget(
			locator.Ref{Kind: locator.KindS3, Bucket: "bucket", Key: "prefix"},
			"s3://bucket/prefix",
			func(target locator.Ref) (int, error) {
				if target.Key != "prefix" {
					t.Fatalf("target = %+v", target)
				}
				return 1, nil
			},
			func(base string) (int, error) {
				t.Fatalf("unexpected local dispatch: %s", base)
				return 0, nil
			},
		)
		if err != nil || warnings != 1 {
			t.Fatalf("dispatchExtractTarget() = (%d, %v), want (1, nil)", warnings, err)
		}
	})

	t.Run("unsupported", func(t *testing.T) {
		_, err := r.dispatchExtractTarget(locator.Ref{}, "bad-target", func(target locator.Ref) (int, error) {
			return 0, nil
		}, func(base string) (int, error) {
			return 0, nil
		})
		if err == nil || !strings.Contains(err.Error(), "unsupported extract target") {
			t.Fatalf("dispatchExtractTarget() err = %v", err)
		}
	})
}

func TestUploadToS3TargetPropagatesS3Options(t *testing.T) {
	r := &Runner{
		storage: &storageRouter{
			s3: fakeS3ArchiveStore{
				uploadStream: func(_ context.Context, ref locator.Ref, body io.Reader, metadata map[string]string) error {
					if ref.Bucket != "bucket" || ref.Key != "prefix/file.txt" {
						t.Fatalf("ref = %+v", ref)
					}
					if ref.CacheControl != "no-store" {
						t.Fatalf("cache-control = %q", ref.CacheControl)
					}
					if !reflect.DeepEqual(ref.ObjectTags, map[string]string{"team": "archive"}) {
						t.Fatalf("object tags = %#v", ref.ObjectTags)
					}
					if !reflect.DeepEqual(metadata, map[string]string{"m": "1"}) {
						t.Fatalf("metadata = %#v", metadata)
					}
					payload, err := io.ReadAll(body)
					if err != nil {
						return err
					}
					if string(payload) != "payload" {
						t.Fatalf("payload = %q", payload)
					}
					return nil
				},
			},
		},
	}

	err := r.uploadToS3Target(
		context.Background(),
		locator.Ref{
			Kind:         locator.KindS3,
			Bucket:       "bucket",
			Key:          "prefix",
			CacheControl: "no-store",
			ObjectTags:   map[string]string{"team": "archive"},
		},
		"file.txt",
		strings.NewReader("payload"),
		map[string]string{"m": "1"},
	)
	if err != nil {
		t.Fatalf("uploadToS3Target() error = %v", err)
	}
}

func TestSafeJoinBlocksTraversal(t *testing.T) {
	_, err := archivepath.SafeJoin("/tmp/out", "../../etc/passwd")
	if err == nil {
		t.Fatalf("expected traversal error")
	}
}

func TestSafeJoinNormal(t *testing.T) {
	p, err := archivepath.SafeJoin("/tmp/out", "dir/file.txt")
	if err != nil {
		t.Fatalf("safeJoin error = %v", err)
	}
	want := filepath.Clean("/tmp/out/dir/file.txt")
	if p != want {
		t.Fatalf("path = %q, want %q", p, want)
	}
}

func TestStripPathComponents(t *testing.T) {
	got, ok := archivepath.StripPathComponents("parent/dir/file.txt", 1)
	if !ok {
		t.Fatalf("expected keep")
	}
	if got != "dir/file.txt" {
		t.Fatalf("got %q, want %q", got, "dir/file.txt")
	}
}

func TestStripPathComponentsDrop(t *testing.T) {
	_, ok := archivepath.StripPathComponents("parent/file.txt", 2)
	if ok {
		t.Fatalf("expected drop")
	}
}

func TestFilterACLLikeXattrs(t *testing.T) {
	in := map[string][]byte{
		"user.mime_type":             []byte("text/plain"),
		"system.posix_acl_access":    []byte("acl-a"),
		"system.posix_acl_default":   []byte("acl-b"),
		"trusted.gotgz.custom-field": []byte("v"),
	}
	got := filterACLLikeXattrs(in)
	if _, ok := got["system.posix_acl_access"]; ok {
		t.Fatalf("acl access xattr should be filtered")
	}
	if _, ok := got["system.posix_acl_default"]; ok {
		t.Fatalf("acl default xattr should be filtered")
	}
	if string(got["user.mime_type"]) != "text/plain" {
		t.Fatalf("non-acl xattr should remain")
	}
	if string(got["trusted.gotgz.custom-field"]) != "v" {
		t.Fatalf("non-acl xattr should remain")
	}
}

func TestResolveMetadataPolicy(t *testing.T) {
	p := resolveMetadataPolicy(cli.Options{Xattrs: true, ACL: false})
	if !p.Xattrs {
		t.Fatalf("Xattrs should be true")
	}
	if p.ACL {
		t.Fatalf("ACL should be false")
	}
}

func TestPrepareMetadataForArchive(t *testing.T) {
	xattrs := map[string][]byte{
		"user.mime_type":           []byte("text/plain"),
		"system.posix_acl_access":  []byte("acl-a"),
		"system.posix_acl_default": []byte("acl-b"),
	}
	acls := map[string][]byte{
		"system.posix_acl_access": []byte("acl-a"),
	}

	t.Run("all disabled", func(t *testing.T) {
		gotX, gotA := prepareMetadataForArchive(xattrs, acls, MetadataPolicy{})
		if gotX != nil {
			t.Fatalf("xattrs expected nil")
		}
		if gotA != nil {
			t.Fatalf("acls expected nil")
		}
	})

	t.Run("xattrs enabled acl disabled", func(t *testing.T) {
		gotX, gotA := prepareMetadataForArchive(xattrs, acls, MetadataPolicy{Xattrs: true, ACL: false})
		if gotA != nil {
			t.Fatalf("acls expected nil")
		}
		if _, ok := gotX["system.posix_acl_access"]; ok {
			t.Fatalf("acl-like xattrs should be filtered")
		}
		if string(gotX["user.mime_type"]) != "text/plain" {
			t.Fatalf("non-acl xattrs should remain")
		}
	})

	t.Run("xattrs disabled acl enabled", func(t *testing.T) {
		gotX, gotA := prepareMetadataForArchive(xattrs, acls, MetadataPolicy{Xattrs: false, ACL: true})
		if gotX != nil {
			t.Fatalf("xattrs expected nil")
		}
		if string(gotA["system.posix_acl_access"]) != "acl-a" {
			t.Fatalf("acls should remain")
		}
	})
}

func TestDecodeMetadataForExtract(t *testing.T) {
	hdr := &tar.Header{Name: "a.txt", Mode: 0o644}
	archive.EncodeXattrToPAX(hdr, map[string][]byte{
		"user.mime_type":          []byte("text/plain"),
		"system.posix_acl_access": []byte("acl-xattr"),
	})
	archive.EncodeACLToPAX(hdr, map[string][]byte{"system.posix_acl_access": []byte("acl-record")})

	t.Run("all disabled", func(t *testing.T) {
		x, a, err := decodeMetadataForExtract(hdr, MetadataPolicy{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if x != nil || a != nil {
			t.Fatalf("expected nil metadata")
		}
	})

	t.Run("xattrs enabled acl disabled", func(t *testing.T) {
		x, a, err := decodeMetadataForExtract(hdr, MetadataPolicy{Xattrs: true})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if a != nil {
			t.Fatalf("acls expected nil")
		}
		if _, ok := x["system.posix_acl_access"]; ok {
			t.Fatalf("acl-like xattr should be filtered")
		}
		if string(x["user.mime_type"]) != "text/plain" {
			t.Fatalf("expected regular xattr")
		}
	})

	t.Run("acl enabled", func(t *testing.T) {
		x, a, err := decodeMetadataForExtract(hdr, MetadataPolicy{ACL: true})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if x != nil {
			t.Fatalf("xattrs expected nil when --xattrs not set")
		}
		if string(a["system.posix_acl_access"]) != "acl-record" {
			t.Fatalf("expected acl record")
		}
	})

	t.Run("malformed xattr reports error", func(t *testing.T) {
		bad := &tar.Header{
			Name:       "bad.txt",
			Mode:       0o644,
			PAXRecords: map[string]string{"GOTGZ.xattr.bad": "***"},
		}
		x, a, err := decodeMetadataForExtract(bad, MetadataPolicy{Xattrs: true})
		if err == nil || !strings.Contains(err.Error(), "decode xattrs") {
			t.Fatalf("decodeMetadataForExtract() err = %v, want decode xattrs failure", err)
		}
		if x != nil {
			t.Fatalf("xattrs should be nil on decode failure")
		}
		if a != nil {
			t.Fatalf("acls should be nil when not requested")
		}
	})
}

func TestLoadExcludePatternsRejectsInvalidInlinePattern(t *testing.T) {
	_, err := archivepath.LoadExcludePatterns([]string{"["}, nil)
	if err == nil {
		t.Fatalf("expected invalid pattern error")
	}
	if !strings.Contains(err.Error(), "invalid exclude pattern") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoadExcludePatternsRejectsInvalidPatternInFile(t *testing.T) {
	tmpDir := t.TempDir()
	patternsFile := filepath.Join(tmpDir, "exclude.txt")
	if err := os.WriteFile(patternsFile, []byte("*.tmp\n[\n"), 0o600); err != nil {
		t.Fatalf("write patterns file: %v", err)
	}

	_, err := archivepath.LoadExcludePatterns(nil, []string{patternsFile})
	if err == nil {
		t.Fatalf("expected invalid pattern error")
	}
	if !strings.Contains(err.Error(), "exclude.txt:2") {
		t.Fatalf("error should include file and line: %v", err)
	}
}

func TestOpenArchiveReaderUsesInjectedLocalStore(t *testing.T) {
	t.Helper()

	var opened locator.Ref
	r := newRunner(
		fakeLocalArchiveStore{
			openReader: func(ref locator.Ref) (io.ReadCloser, localstore.Metadata, error) {
				opened = ref
				return io.NopCloser(strings.NewReader("payload")), localstore.Metadata{Size: 7}, nil
			},
		},
		nil,
		nil,
		io.Discard,
		io.Discard,
	)

	rc, info, err := r.openArchiveReader(context.Background(), locator.Ref{Kind: locator.KindLocal, Raw: "archive.tar", Path: "archive.tar"})
	if err != nil {
		t.Fatalf("openArchiveReader() error = %v", err)
	}
	defer rc.Close() //nolint:errcheck

	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if string(b) != "payload" {
		t.Fatalf("payload = %q, want %q", string(b), "payload")
	}
	if info.Size != 7 || !info.SizeKnown {
		t.Fatalf("archiveReaderInfo = %+v, want size=7 and known=true", info)
	}
	if opened.Path != "archive.tar" {
		t.Fatalf("opened ref = %+v, want path archive.tar", opened)
	}
}
