package engine

import (
	"archive/zip"
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/islishude/gotgz/internal/locator"
)

// addS3MemberZip writes one S3 object as a regular zip member.
func (r *Runner) addS3MemberZip(ctx context.Context, zw *zip.Writer, ref locator.Ref, verbose bool, reporter *progressReporter) (err error) {
	if strings.TrimSpace(ref.Key) == "" {
		return fmt.Errorf("s3 member key cannot be empty: %q", ref.Raw)
	}
	body, _, err := r.s3.OpenReader(ctx, ref)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := body.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()

	hdr := &zip.FileHeader{
		Name:   filepath.ToSlash(ref.Key),
		Method: zip.Deflate,
	}
	hdr.SetMode(0o644)
	hdr.Modified = time.Now()

	w, err := zw.CreateHeader(hdr)
	if err != nil {
		return err
	}
	if _, err := io.Copy(w, newCountingReader(body, reporter)); err != nil {
		return err
	}
	if verbose {
		reporter.beforeExternalLineOutput()
		_, _ = fmt.Fprintln(r.stdout, hdr.Name)
		reporter.afterExternalLineOutput()
	}
	return nil
}

// addLocalPathZip walks a local member and writes entries into the zip archive.
func (r *Runner) addLocalPathZip(ctx context.Context, zw *zip.Writer, member, chdir string, excludes []string, verbose bool, reporter *progressReporter) (int, error) {
	basePath := member
	if chdir != "" {
		basePath = filepath.Join(chdir, member)
	}
	cleanMember := path.Clean(filepath.ToSlash(member))
	warnings := 0
	err := filepath.WalkDir(basePath, func(current string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		rel, err := filepath.Rel(basePath, current)
		if err != nil {
			return err
		}
		archiveName := cleanMember
		if rel != "." {
			archiveName = path.Join(cleanMember, filepath.ToSlash(rel))
		}
		if matchExclude(excludes, archiveName) {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}

		st, err := os.Lstat(current)
		if err != nil {
			return err
		}
		entryName := filepath.ToSlash(archiveName)

		hdr, err := zip.FileInfoHeader(st)
		if err != nil {
			return err
		}
		hdr.Name = entryName
		if st.IsDir() {
			if !strings.HasSuffix(hdr.Name, "/") {
				hdr.Name += "/"
			}
			hdr.Method = zip.Store
		} else if st.Mode()&os.ModeSymlink != 0 {
			hdr.Method = zip.Store
		} else {
			hdr.Method = zip.Deflate
		}
		hdr.Modified = st.ModTime()
		hdr.SetMode(st.Mode())

		w, err := zw.CreateHeader(hdr)
		if err != nil {
			return err
		}
		switch {
		case st.IsDir():
		case st.Mode()&os.ModeSymlink != 0:
			linkTarget, err := os.Readlink(current)
			if err != nil {
				return err
			}
			if _, err := io.WriteString(w, linkTarget); err != nil {
				return err
			}
		case st.Mode().IsRegular():
			f, err := os.Open(current)
			if err != nil {
				return err
			}
			_, err = io.Copy(w, newCountingReader(f, reporter))
			cerr := f.Close()
			if err != nil {
				return err
			}
			if cerr != nil {
				return cerr
			}
		default:
			warnings += r.warnf(reporter, "zip create: unsupported local member type %s for %s; skipping payload", st.Mode().String(), current)
			return nil
		}

		if verbose {
			reporter.beforeExternalLineOutput()
			_, _ = fmt.Fprintln(r.stdout, hdr.Name)
			reporter.afterExternalLineOutput()
		}
		return nil
	})
	return warnings, err
}
