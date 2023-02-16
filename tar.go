package gotgz

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// Fork from https://github.com/mimoo/eureka and fix many memory leak bugs
// https://github.com/mimoo/eureka/blob/master/LICENSE

var Debug bool

func Compress(buf io.WriteCloser, fileList ...string) (err error) {
	// tar -> gzip -> buf
	zr := gzip.NewWriter(buf)
	tw := tar.NewWriter(zr)

	defer func() {
		if err != nil {
			zr.Close()
			tw.Close()
			buf.Close()
		}
	}()

	for _, src := range fileList {
		fi, err := os.Stat(src)
		if err != nil {
			return err
		}

		mode := fi.Mode()
		switch {
		case mode.IsRegular():
			debugf("a %s", src)
			// get header
			header, err := tar.FileInfoHeader(fi, src)
			if err != nil {
				return err
			}
			// write header
			if err := tw.WriteHeader(header); err != nil {
				return err
			}
			// get content
			data, err := os.Open(src)
			if err != nil {
				return err
			}
			if _, err := io.Copy(tw, data); err != nil {
				_ = data.Close()
				return err
			}
			if err := data.Close(); err != nil {
				return err
			}
		case mode.IsDir():
			// walk through every file in the folder
			err := filepath.Walk(src, func(file string, fi os.FileInfo, err error) error {
				if err != nil {
					return err
				}

				// symbolic file will be ignored
				if !fi.IsDir() && !fi.Mode().IsRegular() {
					return nil
				}

				debugf("a %s", file)
				header, err := tar.FileInfoHeader(fi, file)
				if err != nil {
					return err
				}

				header.Name = filepath.ToSlash(file)
				if err := tw.WriteHeader(header); err != nil {
					return err
				}

				// if not a dir, write file content
				if fi.Mode().IsRegular() {
					data, err := os.Open(file)
					if err != nil {
						return err
					}
					if _, err := io.Copy(tw, data); err != nil {
						_ = data.Close()
						return err
					}
					if err := data.Close(); err != nil {
						return err
					}
				}
				return nil
			})
			if err != nil {
				return err
			}
		default:
			return fmt.Errorf("file type %s not supported", mode)
		}
	}

	// close tar
	if err := tw.Close(); err != nil {
		return err
	}
	// close gzip
	if err := zr.Close(); err != nil {
		return err
	}
	return buf.Close()
}

// check for path traversal and correct forward slashes
func isRelPathInvalid(p string) bool {
	return p == "" || strings.Contains(p, `\`) || strings.HasPrefix(p, "/") || strings.Contains(p, "../")
}

func Decompress(src io.ReadCloser, dir string) (err error) {
	defer src.Close()

	zr, err := gzip.NewReader(src)
	if err != nil {
		return err
	}

	tr := tar.NewReader(zr)

	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		target := header.Name
		// validate name against path traversal
		if isRelPathInvalid(target) {
			return fmt.Errorf("file name %q is invalid", target)
		}

		debugf("x %s", target)

		// it's the same with `-C` flag in tar command
		if dir != "" {
			target = filepath.Join(dir, target)
		}

		// check the type
		switch header.Typeflag {
		// if its a dir and it doesn't exist create it (with 0755 permission)
		case tar.TypeDir:
			if _, err := os.Stat(target); err != nil {
				if err := os.MkdirAll(target, 0755); err != nil {
					return err
				}
			}
		// if it's a file create it (with same permission)
		case tar.TypeReg:
			fileToWrite, err := os.OpenFile(target, os.O_CREATE|os.O_RDWR|os.O_TRUNC, os.FileMode(header.Mode))
			if err != nil {
				return err
			}
			// copy over contents
			if _, err := io.Copy(fileToWrite, tr); err != nil {
				return err
			}
			// manually close here after each file operation; defering would cause each file close
			// to wait until all operations have completed.
			if err := fileToWrite.Close(); err != nil {
				return err
			}
		}
	}
	return nil
}
