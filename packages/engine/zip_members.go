package engine

import (
	"archive/zip"
	"context"
	"io"
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
func (r *Runner) writeLocalZipRecord(ctx context.Context, zw zipArchiveWriter, entry *localEntryHandle, verbose bool, reporter *archiveprogress.Reporter) (int, error) {
	record := entry.record
	st := entry.info
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
		if _, err := io.WriteString(w, entry.linkTarget); err != nil {
			return 0, err
		}
	case mode.IsRegular():
		if err := copyLocalEntryPayload(ctx, w, entry, reporter); err != nil {
			return 0, err
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
		reporter.ExternalLinef(r.stdout, "%s\n", hdr.Name)
	}
	return 0, nil
}
