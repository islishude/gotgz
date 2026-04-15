package engine

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"context"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/islishude/gotgz/packages/cli"
)

// recordingTarWriter captures tar headers and payloads written during tests.
type recordingTarWriter struct {
	headers []*tar.Header
	bodies  [][]byte
	current bytes.Buffer
}

// WriteHeader records one tar header and resets the current payload buffer.
func (w *recordingTarWriter) WriteHeader(hdr *tar.Header) error {
	cloned := *hdr
	w.headers = append(w.headers, &cloned)
	w.current.Reset()
	return nil
}

// Write appends file data to the current payload buffer.
func (w *recordingTarWriter) Write(p []byte) (int, error) {
	return w.current.Write(p)
}

// FinishEntry stores the payload accumulated for the current tar member.
func (w *recordingTarWriter) FinishEntry() error {
	w.bodies = append(w.bodies, bytes.Clone(w.current.Bytes()))
	w.current.Reset()
	return nil
}

// Close satisfies the tarArchiveWriter interface for tests.
func (w *recordingTarWriter) Close() error {
	return nil
}

// recordingZipWriter captures zip headers and payloads written during tests.
type recordingZipWriter struct {
	headers []*zip.FileHeader
	bodies  [][]byte
	current *bytes.Buffer
}

// CreateHeader records one zip header and starts a new payload buffer.
func (w *recordingZipWriter) CreateHeader(hdr *zip.FileHeader) (io.Writer, error) {
	cloned := *hdr
	w.headers = append(w.headers, &cloned)
	w.current = &bytes.Buffer{}
	return w.current, nil
}

// FinishEntry stores the payload accumulated for the current zip member.
func (w *recordingZipWriter) FinishEntry() error {
	if w.current == nil {
		w.bodies = append(w.bodies, nil)
		return nil
	}
	w.bodies = append(w.bodies, bytes.Clone(w.current.Bytes()))
	w.current = nil
	return nil
}

// Close satisfies the zipArchiveWriter interface for tests.
func (w *recordingZipWriter) Close() error {
	return nil
}

func TestAddLocalRecordsUsesCurrentTarMetadata(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "file.txt")
	if err := os.WriteFile(path, []byte("payload"), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	runner := &Runner{}
	plan, err := runner.buildCreatePlan(context.Background(), cli.Options{
		Members: []string{"file.txt"},
		Chdir:   root,
	}, nil)
	if err != nil {
		t.Fatalf("buildCreatePlan() error = %v", err)
	}

	if err := os.Chmod(path, 0o755); err != nil {
		t.Fatalf("Chmod() error = %v", err)
	}

	writer := &recordingTarWriter{}
	warnings, err := visitLocalCreateSource(context.Background(), plannedLocalCreateSource{records: plan.members[0].localRecords}, func(record localCreateRecord, info fs.FileInfo) (int, error) {
		return runner.writeLocalTarRecord(context.Background(), writer, record, info, false, MetadataPolicy{}, nil)
	})
	if err != nil {
		t.Fatalf("visitLocalCreateSource() error = %v", err)
	}
	if warnings != 0 {
		t.Fatalf("warnings = %d, want 0", warnings)
	}
	if len(writer.headers) != 1 {
		t.Fatalf("header count = %d, want 1", len(writer.headers))
	}
	if got, want := writer.headers[0].Mode&0o777, int64(0o755); got != want {
		t.Fatalf("header mode = %o, want %o", got, want)
	}
	if len(writer.bodies) != 1 {
		t.Fatalf("body count = %d, want 1", len(writer.bodies))
	}
	if got, want := string(writer.bodies[0]), "payload"; got != want {
		t.Fatalf("payload = %q, want %q", got, want)
	}
}

func TestAddLocalRecordsZipUsesCurrentMetadata(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "file.txt")
	if err := os.WriteFile(path, []byte("payload"), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	runner := &Runner{}
	plan, err := runner.buildCreatePlan(context.Background(), cli.Options{
		Members: []string{"file.txt"},
		Chdir:   root,
	}, nil)
	if err != nil {
		t.Fatalf("buildCreatePlan() error = %v", err)
	}

	if err := os.Chmod(path, 0o755); err != nil {
		t.Fatalf("Chmod() error = %v", err)
	}

	writer := &recordingZipWriter{}
	warnings, err := visitLocalCreateSource(context.Background(), plannedLocalCreateSource{records: plan.members[0].localRecords}, func(record localCreateRecord, info fs.FileInfo) (int, error) {
		return runner.writeLocalZipRecord(context.Background(), writer, record, info, false, nil)
	})
	if err != nil {
		t.Fatalf("visitLocalCreateSource() error = %v", err)
	}
	if warnings != 0 {
		t.Fatalf("warnings = %d, want 0", warnings)
	}
	if len(writer.headers) != 1 {
		t.Fatalf("header count = %d, want 1", len(writer.headers))
	}
	if got, want := writer.headers[0].Mode().Perm(), fs.FileMode(0o755); got != want {
		t.Fatalf("header mode = %o, want %o", got, want)
	}
	if len(writer.bodies) != 1 {
		t.Fatalf("body count = %d, want 1", len(writer.bodies))
	}
	if got, want := string(writer.bodies[0]), "payload"; got != want {
		t.Fatalf("payload = %q, want %q", got, want)
	}
}
