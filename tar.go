package gotgz

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/bmatcuk/doublestar/v4"
)

type ZipWriter func(io.WriteCloser) (io.WriteCloser, error)

type ZipReader func(io.ReadCloser) (io.Reader, error)

type CompressFlags struct {
	Debug    bool
	DryRun   bool
	Relative bool
	Zipper   ZipWriter
	Exclude  []string
}

func Compress(dest io.WriteCloser, flags CompressFlags, fileList ...string) (err error) {
	var zr io.WriteCloser
	if flags.Zipper != nil {
		zr, err = flags.Zipper(dest)
		if err != nil {
			return err
		}
	} else {
		zr = gzip.NewWriter(dest)
	}

	tw := tar.NewWriter(zr)
	defer func() {
		if err != nil {
			zr.Close()
			tw.Close()
			dest.Close()
		}
	}()

	if flags.DryRun {
		debugf(true, "flags %#v", flags)
	}

	var iterater = func(rootPath string) filepath.WalkFunc {
		return func(absPath string, fi os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			isLink, isFile := isSymbolicLink(fi.Mode()), fi.Mode().IsRegular()
			switch {
			case isLink, isFile, fi.Mode().IsDir():
				for _, pattern := range flags.Exclude {
					if match, _ := doublestar.Match(pattern, absPath); match {
						debugf(flags.Debug, "e %s", absPath)
						return nil
					}
				}
				debugf(flags.Debug, "a %s", absPath)
			default:
				debugf(flags.Debug, "i %s", absPath)
				return nil
			}

			if flags.DryRun {
				return nil
			}

			var link = absPath
			if isLink {
				link, err = os.Readlink(absPath)
				if err != nil {
					return err
				}
			}

			// get header
			header, err := tar.FileInfoHeader(fi, link)
			if err != nil {
				return err
			}

			// if we have absPath `../demo/test.txt` and basePath `../demo`
			// we should use `test.txt` as the name
			if flags.Relative || strings.HasPrefix(absPath, "..") {
				rel, err := filepath.Rel(rootPath, absPath)
				if err != nil {
					return err
				}
				header.Name = filepath.ToSlash(rel)
			} else {
				header.Name = filepath.ToSlash(absPath)
			}

			// trim the leading slash
			if filepath.IsAbs(header.Name) {
				header.Name = header.Name[1:]
			}
			if err := tw.WriteHeader(header); err != nil {
				return err
			}

			// if it's a file, write file content
			if isFile {
				data, err := os.Open(absPath)
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
		}
	}

	for _, src := range fileList {
		if err := filepath.Walk(src,
			iterater(filepath.Clean(src))); err != nil {
			return err
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
	return dest.Close()
}

type DecompressFlags struct {
	Debug           bool
	DryRun          bool
	NoSamePerm      bool
	NoSameOwner     bool
	NoSameTime      bool
	NoOverwrite     bool
	StripComponents int
	ZipReader       ZipReader
}

func Decompress(src io.ReadCloser, dir string, flags DecompressFlags) (err error) {
	defer src.Close()

	var zr io.Reader
	if flags.ZipReader != nil {
		zr, err = flags.ZipReader(src)
		if err != nil {
			return err
		}
	} else {
		zr, err = gzip.NewReader(src)
		if err != nil {
			return err
		}
	}

	if flags.DryRun {
		debugf(true, "flags %#v", flags)
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
		if isPathInvalid(target) || filepath.IsAbs(target) {
			return fmt.Errorf("file name %q is invalid", target)
		}

		debugf(flags.Debug, "x %s", target)

		// strip components
		if flags.StripComponents > 0 {
			target = stripComponents(target, flags.StripComponents)
		}

		// it's the same with `-C` flag in tar command
		if dir != "" {
			target = filepath.Join(dir, target)
		}

		if flags.DryRun {
			continue
		}

		switch header.Typeflag {
		case tar.TypeDir:
			var mode = fs.FileMode(header.Mode)
			if flags.NoSamePerm {
				mode = fs.FileMode(0755)
			}
			if err := os.MkdirAll(target, mode); err != nil {
				return err
			}
		case tar.TypeReg:
			var mode = fs.FileMode(header.Mode)
			if flags.NoSamePerm {
				mode = fs.FileMode(0664)
			}

			flag := os.O_CREATE | os.O_RDWR
			if !flags.NoOverwrite {
				flag = flag | os.O_TRUNC
			}

			fileToWrite, err := os.OpenFile(target, flag, mode)
			if err != nil {
				return err
			}
			if _, err := io.Copy(fileToWrite, tr); err != nil {
				return err
			}
			if err := fileToWrite.Close(); err != nil {
				return err
			}
		case tar.TypeSymlink:
			if err := os.Symlink(header.Linkname, target); err != nil {
				return err
			}
		default:
			continue
		}

		if !flags.NoSameOwner {
			if err := os.Chown(target, header.Uid, header.Gid); err != nil {
				return err
			}
		}

		if !flags.NoSameTime {
			if err := os.Chtimes(target, header.AccessTime, header.ModTime); err != nil {
				return err
			}
		}
	}
	return nil
}
