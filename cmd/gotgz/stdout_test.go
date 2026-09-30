package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCreateVerboseStdoutRoundTrip(t *testing.T) {
	root := t.TempDir()
	for _, name := range []string{"a", "b"} {
		if err := os.WriteFile(filepath.Join(root, name), []byte("payload-"+name), 0600); err != nil {
			t.Fatal(err)
		}
	}
	for _, flags := range []string{"-cvf", "-czvf"} {
		t.Run(flags, func(t *testing.T) {
			stdout, stderr, code := runMainProcess(t, flags, "-", "-C", root, "a", "b")
			if code != 0 {
				t.Fatalf("create exit=%d stderr=%s", code, stderr)
			}
			var input io.Reader = bytes.NewBufferString(stdout)
			if flags == "-czvf" {
				gz, err := gzip.NewReader(input)
				if err != nil {
					t.Fatal(err)
				}
				defer func() {
					if err := gz.Close(); err != nil {
						t.Error(err)
					}
				}()
				input = gz
			}
			tr := tar.NewReader(input)
			for _, name := range []string{"a", "b"} {
				hdr, err := tr.Next()
				if err != nil {
					t.Fatal(err)
				}
				body, err := io.ReadAll(tr)
				if err != nil {
					t.Fatal(err)
				}
				if hdr.Name != name || string(body) != "payload-"+name {
					t.Fatalf("entry=%s body=%q", hdr.Name, body)
				}
			}
			if _, err := tr.Next(); err != io.EOF {
				t.Fatalf("trailing entry: %v", err)
			}
			if _, err := io.Copy(io.Discard, input); err != nil {
				t.Fatalf("archive trailer: %v", err)
			}
			if !strings.Contains(stderr, "a\nb\n") {
				t.Fatalf("missing verbose names on stderr: %q", stderr)
			}
		})
	}
}
