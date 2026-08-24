package engine

import (
	"context"
	"errors"
	"io"
	"os"
	"reflect"
	"testing"
)

func TestStreamingMemberSpoolTailsOnlyCompletePublishedFrames(t *testing.T) {
	spool, err := newStreamingMemberSpool(t.TempDir())
	if err != nil {
		t.Fatalf("newStreamingMemberSpool() error = %v", err)
	}
	t.Cleanup(func() {
		if err := spool.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})
	info, err := os.Stat(spool.path)
	if err != nil {
		t.Fatalf("Stat(spool) error = %v", err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("spool mode = %o, want 600", got)
	}

	want := createPlanRecord{Current: "/source/a", ArchiveName: "source/a"}
	result := make(chan createPlanRecord, 1)
	errCh := make(chan error, 1)
	go func() {
		record, err := spool.Next(context.Background())
		result <- record
		errCh <- err
	}()
	if err := spool.Append(want); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if got := <-result; !reflect.DeepEqual(got, want) {
		t.Fatalf("Next() record = %+v, want %+v", got, want)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("Next() error = %v", err)
	}

	spool.Finish(0, nil)
	if _, err := spool.Next(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatalf("Next() after Finish = %v, want io.EOF", err)
	}
}

func TestStreamingMemberSpoolDoesNotExposeUnpublishedPartialFrame(t *testing.T) {
	spool, err := newStreamingMemberSpool(t.TempDir())
	if err != nil {
		t.Fatalf("newStreamingMemberSpool() error = %v", err)
	}
	t.Cleanup(func() { _ = spool.Close() })
	if _, err := spool.file.Write([]byte("partial-frame")); err != nil {
		t.Fatalf("Write(partial) error = %v", err)
	}
	producerErr := errors.New("producer failed during frame append")
	spool.Finish(0, producerErr)
	if _, err := spool.Next(context.Background()); !errors.Is(err, producerErr) {
		t.Fatalf("Next() error = %v, want producer failure", err)
	}
}

func TestStreamingMemberSpoolRejectsPublishedPartialFrame(t *testing.T) {
	spool, err := newStreamingMemberSpool(t.TempDir())
	if err != nil {
		t.Fatalf("newStreamingMemberSpool() error = %v", err)
	}
	t.Cleanup(func() { _ = spool.Close() })
	var frame [9]byte
	frame[7] = 2
	frame[8] = 1
	if _, err := spool.file.Write(frame[:]); err != nil {
		t.Fatalf("Write(frame) error = %v", err)
	}
	spool.mu.Lock()
	spool.published = int64(len(frame))
	spool.producerDone = true
	spool.mu.Unlock()
	if _, err := spool.Next(context.Background()); err == nil {
		t.Fatal("Next() error = nil, want partial-frame rejection")
	}
}

func TestStreamingMemberSpoolCloseRemovesArtifact(t *testing.T) {
	spool, err := newStreamingMemberSpool(t.TempDir())
	if err != nil {
		t.Fatalf("newStreamingMemberSpool() error = %v", err)
	}
	path := spool.path
	spool.Finish(0, nil)
	if err := spool.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("spool path still exists: %v", err)
	}
	if err := spool.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
}
