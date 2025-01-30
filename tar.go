package gotgz

import (
	"archive/tar"
	"context"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/bmatcuk/doublestar/v4"
)

type CompressFlags struct {
	DryRun   bool
	Relative bool
	Archiver Archiver
	Logger   Logger
	Exclude  []string
}

func Compress(ctx context.Context, dest io.WriteCloser, flags CompressFlags, sources ...string) (err error) {
	if flags.Archiver == nil {
		return fmt.Errorf("archiver is nil")
	}

	zr, err := flags.Archiver.Writer(dest)
	if err != nil {
		return err
	}

	var logger = flags.Logger
	if logger == nil {
		logger = slog.Default()
	}

	tw := tar.NewWriter(zr)
	defer func() {
		if err != nil {
			zr.Close()
			tw.Close()
			dest.Close()
		}
	}()

	logger.Debug("flags", "dry-run", flags.DryRun, "relative", flags.Relative,
		"exclude", flags.Exclude, "archiver", flags.Archiver.Name())

	var iterater = func(rootPath string) filepath.WalkFunc {
		return func(absPath string, fi os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			isLink, isFile := IsSymbolicLink(fi.Mode()), fi.Mode().IsRegular()
			switch {
			case isLink, isFile, fi.Mode().IsDir():
				// if we have path rootPath `/data` and absPath `/data/.github/dependabot.yml` and pattern `.github/**`
				// we should use `.github/dependabot.yml` as the path, so the user don't need to use pattern `/data.github/**`
				path := absPath
				rel, err := filepath.Rel(rootPath, absPath)
				if err == nil {
					path = rel
				}
				for _, pattern := range flags.Exclude {
					if doublestar.MatchUnvalidated(pattern, path) {
						logger.Debug("exclude", "target", absPath, "parttern", pattern)
						return nil
					}
				}
				logger.Info("append", "target", absPath)
			default:
				logger.Debug("skip", "target", absPath, "mode", fi.Mode().String())
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
			if flags.Relative || strings.HasPrefix(absPath, "../") {
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
			logger.Debug("tar", "path", header.Name)
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

	for _, src := range sources {
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
	DryRun          bool
	NoSamePerm      bool
	NoSameOwner     bool
	NoSameTime      bool
	NoOverwrite     bool
	StripComponents int
	Archiver        Archiver
	Logger          Logger
}

func Decompress(ctx context.Context, src io.ReadCloser, dir string, flags DecompressFlags) (err error) {
	defer src.Close()

	if flags.Archiver == nil {
		return fmt.Errorf("archiver is nil")
	}

	zr, err := flags.Archiver.Reader(src)
	if err != nil {
		return err
	}

	var logger = flags.Logger
	if logger == nil {
		logger = slog.Default()
	}

	logger.Debug("flags", "dry-run", flags.DryRun, "strip-components", flags.StripComponents, "archiver", flags.Archiver.Name(),
		"no-same-perm", flags.NoSamePerm, "no-same-owner", flags.NoSameOwner, "no-same-time", flags.NoSameTime, "no-overwrite", flags.NoOverwrite)
	tr := tar.NewReader(zr)

	var links = make(map[string]*tar.Header)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		target := header.Name
		if isPathInvalid(target) {
			return fmt.Errorf("file name %q is invalid", target)
		}

		logger.Info("extract", "target", target)

		// strip components
		if flags.StripComponents > 0 {
			target = StripComponents(target, flags.StripComponents)
			logger.Debug("strip", "target", target)
		}

		// it's the same with `-C` flag in tar command
		if dir != "" {
			target = filepath.Join(dir, target)
			logger.Debug("join", "target", target)
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

			if !flags.NoOverwrite {
				// check if the file is exist, if so, skip
				if _, err := os.Stat(target); err == nil {
					logger.Debug("skip", "target", target)
					continue
				}
			}

			fileToWrite, err := os.OpenFile(target, os.O_CREATE|os.O_RDWR|os.O_TRUNC, mode)
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
			// save the link for later
			links[target] = header
			continue
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

	// create symbolic links
	for target, header := range links {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		logger.Debug("link", "source", header.Linkname, "target", target)
		if err := os.Symlink(header.Linkname, target); err != nil {
			return err
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
