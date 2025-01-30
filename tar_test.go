package gotgz

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

type TestFileInfo struct {
	Hash string
	Link string
	Mode uint32
	Uid  int
	Gid  int
}

func GetFileHash(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func TestTar(t *testing.T) {
	type args struct {
		archiver Archiver
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{name: "gzip", args: args{archiver: &GZipArchiver{Level: 9}}, wantErr: false},
		{name: "lz4", args: args{archiver: &Lz4Archiver{Level: 1}}, wantErr: false},
		{name: "zstd", args: args{archiver: &ZstdArchiver{Level: 0}}, wantErr: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			create, extract := t.TempDir(), t.TempDir()
			destPath := filepath.Join(create, "testdata.tar.gz")

			{
				file, err := os.Create(destPath)
				if err != nil {
					t.Fatal(err)
				}

				flags := CompressFlags{
					Archiver: tt.args.archiver,
					Relative: true,
					Exclude:  []string{"parent/.exclude/**"},
				}
				if err := Compress(file, flags, "testdata"); err != nil {
					t.Fatal(err)
				}
			}

			{
				source, err := os.Open(destPath)
				if err != nil {
					t.Fatal(err)
				}
				flags := DecompressFlags{
					Archiver:        tt.args.archiver,
					NoSamePerm:      false,
					NoSameOwner:     false,
					NoSameTime:      false,
					NoOverwrite:     false,
					StripComponents: 1,
				}

				if err := Decompress(source, extract, flags); err != nil {
					t.Fatal(err)
				}
			}

			{
				var count = 0
				origin := make(map[string]TestFileInfo)
				err := filepath.Walk("testdata", func(path string, info os.FileInfo, err error) error {
					if err != nil {
						return err
					}

					if strings.HasPrefix(path, "testdata/parent/.exclude") {
						return nil
					}

					rel, err := filepath.Rel("testdata", path)
					if err != nil {
						return err
					}

					var hash string
					if info.Mode().IsRegular() {
						hash, err = GetFileHash(path)
						if err != nil {
							return err
						}
					}

					var file = TestFileInfo{
						Hash: hash,
						Mode: uint32(info.Mode()),
					}

					if sys, ok := info.Sys().(*syscall.Stat_t); ok {
						file.Uid = int(sys.Uid)
						file.Gid = int(sys.Gid)
					}

					if isSymbolicLink(info.Mode()) {
						file.Link, err = os.Readlink(path)
						if err != nil {
							return err
						}
					}
					origin[stripComponents(rel, 1)] = file
					count++
					return nil
				})
				if err != nil {
					t.Fatal(err)
				}

				processed := make(map[string]TestFileInfo)
				err = filepath.Walk(extract, func(path string, info os.FileInfo, err error) error {
					if err != nil {
						return err
					}

					var hash string
					if info.Mode().IsRegular() {
						hash, err = GetFileHash(path)
						if err != nil {
							return err
						}
					}

					var file = TestFileInfo{
						Hash: hash,
						Mode: uint32(info.Mode()),
					}

					rel, err := filepath.Rel(extract, path)
					if err != nil {
						return err
					}

					if sys, ok := info.Sys().(*syscall.Stat_t); ok {
						file.Uid = int(sys.Uid)
						file.Gid = int(sys.Gid)
					}

					if isSymbolicLink(info.Mode()) {
						file.Link, err = os.Readlink(path)
						if err != nil {
							return err
						}
					}

					processed[rel] = file
					return nil
				})

				if err != nil {
					t.Fatal(err)
				}

				if len(origin) != len(processed) {
					t.Fatalf("origin: %d, processed: %d", len(origin), len(processed))
				}

				if len(origin) != count {
					t.Fatalf("origin: %d, count: %d", len(origin), count)
				}

				for rel, originFile := range origin {
					processedFile, ok := processed[rel]
					if !ok {
						t.Fatalf("file %s not found", rel)
					}

					if originFile.Hash != processedFile.Hash {
						t.Fatalf("file %s hash not match", rel)
					}

					if originFile.Mode != processedFile.Mode {
						t.Fatalf("file %s mode not match", rel)
					}

					if originFile.Uid != processedFile.Uid {
						t.Fatalf("file %s uid not match", rel)
					}

					if originFile.Gid != processedFile.Gid {
						t.Fatalf("file %s gid not match", rel)
					}
				}
			}
		})
	}
}
