package engine

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/islishude/gotgz/packages/locator"
	"github.com/islishude/gotgz/packages/storage/s3"
)

func TestStreamS3MemberToArchive(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		var gotName string
		var gotSize int64
		var gotBody string

		r := &Runner{
			storage: &storageRouter{
				s3: fakeS3ArchiveStore{
					openReader: func(_ context.Context, _ locator.Ref) (io.ReadCloser, s3.Metadata, error) {
						return io.NopCloser(strings.NewReader("s3body")), s3.Metadata{Size: 6}, nil
					},
				},
			},
			stderr: io.Discard,
			stdout: io.Discard,
		}

		err := r.streamS3MemberToArchive(context.Background(), locator.Ref{Kind: locator.KindS3, Bucket: "b", Key: "objects/file.txt"}, false, nil, func(name string, size int64, _ time.Time, body io.Reader) error {
			gotName = name
			gotSize = size
			payload, err := io.ReadAll(body)
			if err != nil {
				return err
			}
			gotBody = string(payload)
			return nil
		})
		if err != nil {
			t.Fatalf("streamS3MemberToArchive() error = %v", err)
		}
		if gotName != "objects/file.txt" || gotSize != 6 || gotBody != "s3body" {
			t.Fatalf("got name=%q size=%d body=%q", gotName, gotSize, gotBody)
		}
	})

	t.Run("open error", func(t *testing.T) {
		wantErr := errors.New("s3 open failed")
		r := &Runner{storage: &storageRouter{s3: fakeS3ArchiveStore{openReader: func(_ context.Context, _ locator.Ref) (io.ReadCloser, s3.Metadata, error) {
			return nil, s3.Metadata{}, wantErr
		}}}}
		if err := r.streamS3MemberToArchive(context.Background(), locator.Ref{Kind: locator.KindS3, Key: "k"}, false, nil, nil); !errors.Is(err, wantErr) {
			t.Fatalf("err = %v, want %v", err, wantErr)
		}
	})

	t.Run("write error", func(t *testing.T) {
		wantErr := errors.New("write failed")
		r := &Runner{storage: &storageRouter{s3: fakeS3ArchiveStore{openReader: func(_ context.Context, _ locator.Ref) (io.ReadCloser, s3.Metadata, error) {
			return io.NopCloser(strings.NewReader("data")), s3.Metadata{}, nil
		}}}}
		if err := r.streamS3MemberToArchive(context.Background(), locator.Ref{Kind: locator.KindS3, Key: "k"}, false, nil, func(_ string, _ int64, _ time.Time, _ io.Reader) error {
			return wantErr
		}); !errors.Is(err, wantErr) {
			t.Fatalf("err = %v, want %v", err, wantErr)
		}
	})

	t.Run("close error", func(t *testing.T) {
		wantErr := errors.New("close failed")
		r := &Runner{storage: &storageRouter{s3: fakeS3ArchiveStore{openReader: func(_ context.Context, _ locator.Ref) (io.ReadCloser, s3.Metadata, error) {
			return &errCloser{Reader: strings.NewReader("x"), closeErr: wantErr}, s3.Metadata{}, nil
		}}}}
		if err := r.streamS3MemberToArchive(context.Background(), locator.Ref{Kind: locator.KindS3, Key: "k"}, false, nil, func(_ string, _ int64, _ time.Time, body io.Reader) error {
			_, _ = io.ReadAll(body)
			return nil
		}); !errors.Is(err, wantErr) {
			t.Fatalf("err = %v, want %v", err, wantErr)
		}
	})
}

type errCloser struct {
	io.Reader
	closeErr error
}

func (e *errCloser) Close() error { return e.closeErr }
