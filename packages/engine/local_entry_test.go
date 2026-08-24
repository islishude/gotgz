package engine

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
)

func TestCopyLocalEntryPayloadUsesRefreshedSizeSnapshot(t *testing.T) {
	t.Run("grow is ignored", func(t *testing.T) {
		root := t.TempDir()
		path := filepath.Join(root, "member")
		if err := os.WriteFile(path, []byte("base"), 0o600); err != nil {
			t.Fatalf("WriteFile(base) error = %v", err)
		}
		entry, err := openLocalEntry(localCreateRecord{current: path, archiveName: "member"}, fs.FileMode(0))
		if err != nil {
			t.Fatalf("openLocalEntry() error = %v", err)
		}
		defer func() { _ = entry.Close() }()
		file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
		if err != nil {
			t.Fatalf("OpenFile(append) error = %v", err)
		}
		if _, err := io.WriteString(file, "-growth"); err != nil {
			t.Fatalf("WriteString(growth) error = %v", err)
		}
		if err := file.Close(); err != nil {
			t.Fatalf("Close(append) error = %v", err)
		}
		var payload bytes.Buffer
		if err := copyLocalEntryPayload(context.Background(), &payload, entry, nil); err != nil {
			t.Fatalf("copyLocalEntryPayload() error = %v", err)
		}
		if got := payload.String(); got != "base" {
			t.Fatalf("payload = %q, want refreshed-size snapshot", got)
		}
	})

	t.Run("truncate fails short read", func(t *testing.T) {
		root := t.TempDir()
		path := filepath.Join(root, "member")
		if err := os.WriteFile(path, []byte("payload"), 0o600); err != nil {
			t.Fatalf("WriteFile(payload) error = %v", err)
		}
		entry, err := openLocalEntry(localCreateRecord{current: path, archiveName: "member"}, fs.FileMode(0))
		if err != nil {
			t.Fatalf("openLocalEntry() error = %v", err)
		}
		defer func() { _ = entry.Close() }()
		if err := os.Truncate(path, 2); err != nil {
			t.Fatalf("Truncate() error = %v", err)
		}
		if err := copyLocalEntryPayload(context.Background(), io.Discard, entry, nil); !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("copyLocalEntryPayload() error = %v, want io.ErrUnexpectedEOF", err)
		}
	})
}
