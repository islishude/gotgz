package engine

import (
	"archive/zip"
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/islishude/gotgz/packages/archiveprogress"
	"github.com/islishude/gotgz/packages/archiveutil"
	"github.com/islishude/gotgz/packages/locator"
)

// addS3ZipMember writes one S3 object as a regular zip member.
func (r *Runner) addS3ZipMember(ctx context.Context, zw zipArchiveWriter, ref locator.Ref, verbose bool, reporter *archiveprogress.Reporter) (err error) {
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
		if _, err := archiveutil.CopyWithContext(ctx, w, body); err != nil {
			return err
		}
		return zw.FinishEntry()
	})
}

// writeLocalZipRecord writes one local filesystem record into the zip archive.
func (r *Runner) writeLocalZipRecord(ctx context.Context, zw zipArchiveWriter, record localCreateRecord, st fs.FileInfo, verbose bool, reporter *archiveprogress.Reporter) (int, error) {
	mode := st.Mode()
	isDir := st.IsDir()
	isSymlink := mode&os.ModeSymlink != 0
	entryName := filepath.ToSlash(record.archiveName)

	hdr, err := zip.FileInfoHeader(st)
	if err != nil {
		return 0, err
	}
	hdr.Name = entryName
	if isDir {
		if !strings.HasSuffix(hdr.Name, "/") {
			hdr.Name += "/"
		}
		hdr.Method = zip.Store
	} else if isSymlink {
		hdr.Method = zip.Store
	} else {
		hdr.Method = zip.Deflate
	}
	hdr.Modified = st.ModTime()
	hdr.SetMode(mode)

	w, err := zw.CreateHeader(hdr)
	if err != nil {
		return 0, err
	}
	switch {
	case isDir:
	case isSymlink:
		linkTarget, err := os.Readlink(record.current)
		if err != nil {
			return 0, err
		}
		if _, err := io.WriteString(w, linkTarget); err != nil {
			return 0, err
		}
	case mode.IsRegular():
		f, err := os.Open(record.current)
		if err != nil {
			return 0, err
		}
		_, err = archiveutil.CopyWithContext(ctx, w, archiveprogress.NewCountingReader(f, reporter))
		cerr := f.Close()
		if err != nil {
			return 0, err
		}
		if cerr != nil {
			return 0, cerr
		}
	default:
		if err := zw.FinishEntry(); err != nil {
			return 0, err
		}
		return r.warnf(reporter, "zip create: unsupported local member type %s for %s; skipping payload", mode.String(), record.current), nil
	}
	if err := zw.FinishEntry(); err != nil {
		return 0, err
	}

	if verbose {
		reporter.BeforeExternalLineOutput()
		_, _ = fmt.Fprintln(r.stdout, hdr.Name)
		reporter.AfterExternalLineOutput()
	}
	return 0, nil
}
