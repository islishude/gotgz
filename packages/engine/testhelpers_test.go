package engine

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/islishude/gotgz/packages/locator"
	httpstore "github.com/islishude/gotgz/packages/storage/http"
	localstore "github.com/islishude/gotgz/packages/storage/local"
	s3store "github.com/islishude/gotgz/packages/storage/s3"
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

type fakeS3ArchiveStore struct {
	openReader   func(ctx context.Context, ref locator.Ref) (io.ReadCloser, s3store.Metadata, error)
	stat         func(ctx context.Context, ref locator.Ref) (s3store.Metadata, error)
	openWriter   func(ctx context.Context, ref locator.Ref, metadata map[string]string) (io.WriteCloser, error)
	uploadStream func(ctx context.Context, ref locator.Ref, body io.Reader, metadata map[string]string) error
	listPrefix   func(ctx context.Context, bucket string, prefix string) ([]s3store.ListedObject, error)
}

func (f fakeS3ArchiveStore) OpenReader(ctx context.Context, ref locator.Ref) (io.ReadCloser, s3store.Metadata, error) {
	if f.openReader == nil {
		return nil, s3store.Metadata{}, nil
	}
	return f.openReader(ctx, ref)
}

func (f fakeS3ArchiveStore) Stat(ctx context.Context, ref locator.Ref) (s3store.Metadata, error) {
	if f.stat == nil {
		return s3store.Metadata{}, nil
	}
	return f.stat(ctx, ref)
}

func (f fakeS3ArchiveStore) OpenWriter(ctx context.Context, ref locator.Ref, metadata map[string]string) (io.WriteCloser, error) {
	if f.openWriter == nil {
		return nil, nil
	}
	return f.openWriter(ctx, ref, metadata)
}

func (f fakeS3ArchiveStore) UploadStream(ctx context.Context, ref locator.Ref, body io.Reader, metadata map[string]string) error {
	if f.uploadStream == nil {
		return nil
	}
	return f.uploadStream(ctx, ref, body, metadata)
}

func (f fakeS3ArchiveStore) ListPrefix(ctx context.Context, bucket string, prefix string) ([]s3store.ListedObject, error) {
	if f.listPrefix == nil {
		return nil, nil
	}
	return f.listPrefix(ctx, bucket, prefix)
}

type fakeHTTPArchiveStore struct {
	openReader func(ctx context.Context, ref locator.Ref) (io.ReadCloser, httpstore.Metadata, error)
}

func (f fakeHTTPArchiveStore) OpenReader(ctx context.Context, ref locator.Ref) (io.ReadCloser, httpstore.Metadata, error) {
	if f.openReader == nil {
		return nil, httpstore.Metadata{}, nil
	}
	return f.openReader(ctx, ref)
}

type fakeS3ZipArchiveStore struct {
	fakeS3ArchiveStore
	openRange func(ctx context.Context, ref locator.Ref, offset int64, length int64) (io.ReadCloser, error)
}

func (f fakeS3ZipArchiveStore) OpenRangeReader(ctx context.Context, ref locator.Ref, offset int64, length int64) (io.ReadCloser, error) {
	if f.openRange == nil {
		return nil, errors.New("fakeS3ZipArchiveStore: OpenRangeReader not implemented")
	}
	return f.openRange(ctx, ref, offset, length)
}

type fakeHTTPZipArchiveStore struct {
	fakeHTTPArchiveStore
	openRange func(ctx context.Context, ref locator.Ref, offset int64, length int64) (io.ReadCloser, error)
}

func (f fakeHTTPZipArchiveStore) OpenRangeReader(ctx context.Context, ref locator.Ref, offset int64, length int64) (io.ReadCloser, error) {
	if f.openRange == nil {
		return nil, errors.New("fakeHTTPZipArchiveStore: OpenRangeReader not implemented")
	}
	return f.openRange(ctx, ref, offset, length)
}

type fakeWriteCloser struct {
	bytes.Buffer
	closeErr error
}

func (f *fakeWriteCloser) Close() error { return f.closeErr }

type trackingReadCloser struct {
	io.Reader
	readCalls  int
	closeCalls int
}

func (r *trackingReadCloser) Read(p []byte) (int, error) {
	r.readCalls++
	return r.Reader.Read(p)
}

func (r *trackingReadCloser) Close() error {
	r.closeCalls++
	return nil
}

type blockingChunkReader struct {
	started   chan struct{}
	startOnce sync.Once
	release   chan struct{}
	remaining int
	chunk     []byte
}

func newBlockingChunkReader(chunks, chunkSize int) *blockingChunkReader {
	if chunkSize <= 0 {
		chunkSize = 1
	}
	return &blockingChunkReader{
		started:   make(chan struct{}),
		release:   make(chan struct{}, chunks),
		remaining: chunks,
		chunk:     bytes.Repeat([]byte{'x'}, chunkSize),
	}
}

func (r *blockingChunkReader) Read(p []byte) (int, error) {
	if r.remaining <= 0 {
		return 0, io.EOF
	}
	r.startOnce.Do(func() {
		close(r.started)
	})
	<-r.release
	r.remaining--
	return copy(p, r.chunk), nil
}

func (r *blockingChunkReader) allowRead() {
	r.release <- struct{}{}
}

func (r *blockingChunkReader) waitForStart(t *testing.T) {
	t.Helper()
	select {
	case <-r.started:
	case <-time.After(time.Second):
		t.Fatal("reader did not start")
	}
}

type tarEntry struct {
	hdr  *tar.Header
	body string
}

func newTarHeader(name string, size int64, mode int64) *tar.Header {
	return &tar.Header{
		Name:     name,
		Mode:     mode,
		Size:     size,
		Typeflag: tar.TypeReg,
		Format:   tar.FormatPAX,
	}
}

func newTarReaderFromEntries(t *testing.T, entries []tarEntry) *tar.Reader {
	t.Helper()

	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, entry := range entries {
		if err := tw.WriteHeader(entry.hdr); err != nil {
			t.Fatalf("WriteHeader(%q): %v", entry.hdr.Name, err)
		}
		if _, err := io.WriteString(tw, entry.body); err != nil {
			t.Fatalf("Write(%q): %v", entry.hdr.Name, err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("close tar writer: %v", err)
	}
	return tar.NewReader(bytes.NewReader(buf.Bytes()))
}

func tarArchiveBytes(t *testing.T, files map[string]string) []byte {
	t.Helper()

	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for name, content := range files {
		hdr := &tar.Header{
			Name:     name,
			Mode:     0o644,
			Size:     int64(len(content)),
			Typeflag: tar.TypeReg,
			Format:   tar.FormatPAX,
		}
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatalf("WriteHeader(%q): %v", name, err)
		}
		if _, err := io.WriteString(tw, content); err != nil {
			t.Fatalf("Write(%q): %v", name, err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("close tar writer: %v", err)
	}
	return buf.Bytes()
}

func zipArchiveBytes(t *testing.T, files map[string]string) []byte {
	t.Helper()

	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	for name, content := range files {
		w, err := zw.Create(name)
		if err != nil {
			t.Fatalf("Create(%q): %v", name, err)
		}
		if _, err := io.WriteString(w, content); err != nil {
			t.Fatalf("Write(%q): %v", name, err)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("close zip writer: %v", err)
	}
	return buf.Bytes()
}

type fixtureEntry struct {
	path    string
	body    string
	mode    os.FileMode
	symlink string
	isDir   bool
}

func writeFixtureTree(t *testing.T, root string, entries []fixtureEntry) {
	t.Helper()
	for _, entry := range entries {
		targetPath := filepath.Join(root, entry.path)
		if entry.isDir {
			if err := os.MkdirAll(targetPath, entry.mode); err != nil {
				t.Fatalf("mkdir %s: %v", entry.path, err)
			}
			continue
		}
		if entry.symlink != "" {
			if err := os.MkdirAll(filepath.Dir(targetPath), 0o755); err != nil {
				t.Fatalf("mkdir parent for %s: %v", entry.path, err)
			}
			if err := os.Symlink(entry.symlink, targetPath); err != nil {
				t.Fatalf("symlink %s -> %s: %v", entry.path, entry.symlink, err)
			}
			continue
		}
		if err := os.MkdirAll(filepath.Dir(targetPath), 0o755); err != nil {
			t.Fatalf("mkdir parent for %s: %v", entry.path, err)
		}
		mode := entry.mode
		if mode == 0 {
			mode = 0o644
		}
		if err := os.WriteFile(targetPath, []byte(entry.body), mode); err != nil {
			t.Fatalf("write %s: %v", entry.path, err)
		}
	}
}
