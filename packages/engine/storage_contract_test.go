package engine

import (
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"io"
	"path/filepath"
	"testing"

	"github.com/islishude/gotgz/packages/archiveutil"
	"github.com/islishude/gotgz/packages/cli"
	"github.com/islishude/gotgz/packages/locator"
	localstore "github.com/islishude/gotgz/packages/storage/local"
	s3store "github.com/islishude/gotgz/packages/storage/s3"
)

type recordingWriteSession struct {
	bytes.Buffer
	commits, aborts int
	cause           error
}

func (s *recordingWriteSession) Commit() error         { s.commits++; return nil }
func (s *recordingWriteSession) Close() error          { return s.Commit() }
func (s *recordingWriteSession) Abort(err error) error { s.aborts++; s.cause = err; return nil }

func TestCreateAbortsRequiredStorageSessionWithoutCommitting(t *testing.T) {
	for _, backend := range []string{"local", "s3"} {
		for _, format := range []string{"tar", "zip"} {
			t.Run(backend+"/"+format, func(t *testing.T) {
				want := errors.New("source failed after preflight")
				session := &recordingWriteSession{}
				local := fakeLocalArchiveStore{beginWriter: func(locator.Ref) (localstore.WriteSession, error) { return session, nil }}
				s3 := fakeS3ArchiveStore{
					stat: func(context.Context, locator.Ref) (s3store.Metadata, error) { return s3store.Metadata{Size: 1}, nil },
					openReader: func(context.Context, locator.Ref) (io.ReadCloser, s3store.Metadata, error) {
						return nil, s3store.Metadata{}, want
					},
					beginWriter: func(context.Context, locator.Ref, map[string]string) (s3store.WriteSession, error) {
						return session, nil
					},
				}
				destination := filepath.Join(t.TempDir(), "archive."+format)
				if backend == "s3" {
					destination = "s3://bucket/archive." + format
				}
				r := newRunner(local, s3, nil, io.Discard, io.Discard)
				result := r.Run(context.Background(), cli.Options{Mode: cli.ModeCreate, Archive: destination, Members: []string{"s3://bucket/input"}})
				if result.ExitCode != ExitFatal || !errors.Is(result.Err, want) {
					t.Fatalf("Run() = %+v", result)
				}
				if session.commits != 0 || session.aborts != 1 || !errors.Is(session.cause, want) {
					t.Fatalf("session commits=%d aborts=%d cause=%v", session.commits, session.aborts, session.cause)
				}
			})
		}
	}
}

// These stores intentionally expose only the old, unfenced range method.
type unfencedHTTPStore struct {
	fakeHTTPArchiveStore
	calls *int
}

func (s unfencedHTTPStore) OpenRangeReader(context.Context, locator.Ref, int64, int64) (io.ReadCloser, error) {
	*s.calls++
	return nil, errors.New("unfenced range must never be called")
}

type unfencedS3Store struct {
	fakeS3ArchiveStore
	calls *int
}

func (s unfencedS3Store) OpenRangeReader(context.Context, locator.Ref, int64, int64) (io.ReadCloser, error) {
	*s.calls++
	return nil, errors.New("unfenced range must never be called")
}

func TestZipStagesOriginalStreamWhenStoreCannotFenceRanges(t *testing.T) {
	for _, kind := range []locator.Kind{locator.KindHTTP, locator.KindS3} {
		t.Run(string(kind), func(t *testing.T) {
			payload := zipArchiveBytes(t, map[string]string{"file": "payload"})
			stream := &trackingReadCloser{Reader: bytes.NewReader(payload)}
			calls := 0
			r := newRunner(nil, unfencedS3Store{calls: &calls}, unfencedHTTPStore{calls: &calls}, io.Discard, io.Discard)
			_, err := r.withZipReader(context.Background(), locator.Ref{Kind: kind, Key: "archive.zip"}, stream,
				archiveReaderInfo{Size: int64(len(payload)), SizeKnown: true, Snapshot: archiveutil.Snapshot{Size: int64(len(payload)), ETag: `"original"`}}, nil,
				func(zr *zip.Reader) (int, error) {
					if len(zr.File) != 1 || zr.File[0].Name != "file" {
						t.Fatalf("files = %+v", zr.File)
					}
					return 0, nil
				})
			if err != nil {
				t.Fatal(err)
			}
			if calls != 0 || stream.readCalls == 0 {
				t.Fatalf("range calls=%d stream reads=%d", calls, stream.readCalls)
			}
		})
	}
}
