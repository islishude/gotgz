package engine

import (
	"archive/zip"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/islishude/gotgz/internal/locator"
)

// addS3MemberZip writes one S3 object as a regular zip member.
func (r *Runner) addS3MemberZip(ctx context.Context, zw *zip.Writer, ref locator.Ref, verbose bool, reporter *progressReporter) (err error) {
	return r.streamS3MemberToArchive(ctx, ref, verbose, reporter, func(name string, _ int64, modified time.Time, body io.Reader) error {
		hdr := &zip.FileHeader{
			Name:   name,
			Method: zip.Deflate,
		}
		hdr.SetMode(0o644)
		hdr.Modified = modified

		w, err := zw.CreateHeader(hdr)
		if err != nil {
			return err
		}
		_, err = copyWithContext(ctx, w, body)
		return err
	})
}

// addLocalPathZip walks a local member and writes entries into the zip archive.
func (r *Runner) addLocalPathZip(ctx context.Context, zw *zip.Writer, member, chdir string, excludes []string, verbose bool, reporter *progressReporter) (int, error) {
	warnings := 0
	err := walkLocalCreateMember(ctx, member, chdir, excludes, func(entry localCreateEntry) error {
		st := entry.info
		entryName := filepath.ToSlash(entry.archiveName)

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
			linkTarget, err := os.Readlink(entry.current)
			if err != nil {
				return err
			}
			if _, err := io.WriteString(w, linkTarget); err != nil {
				return err
			}
		case st.Mode().IsRegular():
			f, err := os.Open(entry.current)
			if err != nil {
				return err
			}
			_, err = copyWithContext(ctx, w, newCountingReader(f, reporter))
			cerr := f.Close()
			if err != nil {
				return err
			}
			if cerr != nil {
				return cerr
			}
		default:
			warnings += r.warnf(reporter, "zip create: unsupported local member type %s for %s; skipping payload", st.Mode().String(), entry.current)
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
