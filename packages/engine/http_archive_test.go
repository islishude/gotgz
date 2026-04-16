package engine

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/islishude/gotgz/packages/cli"
	httpstore "github.com/islishude/gotgz/packages/storage/http"
	localstore "github.com/islishude/gotgz/packages/storage/local"
)

func TestHTTPArchivesStayInDefaultLayer(t *testing.T) {
	t.Run("list tar", func(t *testing.T) {
		archiveBytes := tarArchiveBytes(t, map[string]string{"files/one.txt": "one", "files/two.txt": "two"})
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write(archiveBytes)
		}))
		defer server.Close()

		var stdout bytes.Buffer
		r := newRunner(&localstore.ArchiveStore{}, nil, httpstore.New(), &stdout, io.Discard)
		if got := r.Run(context.Background(), cli.Options{Mode: cli.ModeList, Archive: server.URL + "/archive.tar"}); got.ExitCode != ExitSuccess {
			t.Fatalf("list exit=%d err=%v", got.ExitCode, got.Err)
		}
		if !strings.Contains(stdout.String(), "files/one.txt") || !strings.Contains(stdout.String(), "files/two.txt") {
			t.Fatalf("stdout = %q", stdout.String())
		}
	})

	t.Run("extract tar", func(t *testing.T) {
		archiveBytes := tarArchiveBytes(t, map[string]string{"dir/hello.txt": "hello-http"})
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write(archiveBytes)
		}))
		defer server.Close()

		outDir := t.TempDir()
		r := newRunner(&localstore.ArchiveStore{}, nil, httpstore.New(), io.Discard, io.Discard)
		if got := r.Run(context.Background(), cli.Options{Mode: cli.ModeExtract, Archive: server.URL + "/archive.tar", Chdir: outDir}); got.ExitCode != ExitSuccess {
			t.Fatalf("extract exit=%d err=%v", got.ExitCode, got.Err)
		}
		b, err := os.ReadFile(filepath.Join(outDir, "dir", "hello.txt"))
		if err != nil {
			t.Fatalf("ReadFile() error = %v", err)
		}
		if string(b) != "hello-http" {
			t.Fatalf("hello.txt = %q, want hello-http", string(b))
		}
	})

	t.Run("list zip", func(t *testing.T) {
		archiveBytes := zipArchiveBytes(t, map[string]string{"files/one.txt": "one"})
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write(archiveBytes)
		}))
		defer server.Close()

		var stdout bytes.Buffer
		r := newRunner(&localstore.ArchiveStore{}, nil, httpstore.New(), &stdout, io.Discard)
		if got := r.Run(context.Background(), cli.Options{Mode: cli.ModeList, Archive: server.URL + "/archive.zip"}); got.ExitCode != ExitSuccess {
			t.Fatalf("list exit=%d err=%v", got.ExitCode, got.Err)
		}
		if !strings.Contains(stdout.String(), "files/one.txt") {
			t.Fatalf("stdout = %q", stdout.String())
		}
	})

	t.Run("extract zip", func(t *testing.T) {
		archiveBytes := zipArchiveBytes(t, map[string]string{"dir/hello.txt": "hello-zip-http"})
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write(archiveBytes)
		}))
		defer server.Close()

		outDir := t.TempDir()
		r := newRunner(&localstore.ArchiveStore{}, nil, httpstore.New(), io.Discard, io.Discard)
		if got := r.Run(context.Background(), cli.Options{Mode: cli.ModeExtract, Archive: server.URL + "/archive.zip", Chdir: outDir}); got.ExitCode != ExitSuccess {
			t.Fatalf("extract exit=%d err=%v", got.ExitCode, got.Err)
		}
		b, err := os.ReadFile(filepath.Join(outDir, "dir", "hello.txt"))
		if err != nil {
			t.Fatalf("ReadFile() error = %v", err)
		}
		if string(b) != "hello-zip-http" {
			t.Fatalf("hello.txt = %q, want hello-zip-http", string(b))
		}
	})

	t.Run("create to http target fails", func(t *testing.T) {
		r := newRunner(&localstore.ArchiveStore{}, nil, httpstore.New(), io.Discard, io.Discard)
		got := r.Run(context.Background(), cli.Options{Mode: cli.ModeCreate, Archive: "https://example.test/archive.tar", Members: []string{"missing"}})
		if got.ExitCode != ExitFatal || got.Err == nil || !strings.Contains(got.Err.Error(), "source-only") {
			t.Fatalf("create result = %+v, want source-only failure", got)
		}
	})
}
