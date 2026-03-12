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

	"github.com/islishude/gotgz/packages/archivepath"
	"github.com/islishude/gotgz/packages/archiveprogress"
	"github.com/islishude/gotgz/packages/archiveutil"
	"github.com/islishude/gotgz/packages/locator"
)

// addS3MemberZip writes one S3 object as a regular zip member.
func (r *Runner) addS3MemberZip(ctx context.Context, zw zipArchiveWriter, ref locator.Ref, verbose bool, reporter *archiveprogress.Reporter) (err error) {
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

// addLocalPathZip walks a local member and writes entries into the zip archive.
func (r *Runner) addLocalPathZip(ctx context.Context, zw zipArchiveWriter, member, chdir string, excludeMatcher *archivepath.CompiledPathMatcher, verbose bool, reporter *archiveprogress.Reporter) (int, error) {
	warnings := 0
	err := walkLocalCreateMember(ctx, member, chdir, excludeMatcher, func(entry localCreateEntry) error {
		w, err := r.writeLocalZipEntry(ctx, zw, entry, verbose, reporter)
		warnings += w
		return err
	})
	return warnings, err
}

// addLocalEntriesZip writes a pre-scanned set of local filesystem entries into
// the zip archive.
func (r *Runner) addLocalEntriesZip(ctx context.Context, zw zipArchiveWriter, entries []localCreateEntry, verbose bool, reporter *archiveprogress.Reporter) (int, error) {
	warnings := 0
	for _, entry := range entries {
		select {
		case <-ctx.Done():
			return warnings, ctx.Err()
		default:
		}
		w, err := r.writeLocalZipEntry(ctx, zw, entry, verbose, reporter)
		warnings += w
		if err != nil {
			return warnings, err
		}
	}
	return warnings, nil
}

// writeLocalZipEntry writes one local filesystem entry into the zip archive.
func (r *Runner) writeLocalZipEntry(ctx context.Context, zw zipArchiveWriter, entry localCreateEntry, verbose bool, reporter *archiveprogress.Reporter) (int, error) {
	st := entry.info
	entryName := filepath.ToSlash(entry.archiveName)

	hdr, err := zip.FileInfoHeader(st)
	if err != nil {
		return 0, err
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
		return 0, err
	}
	switch {
	case st.IsDir():
	case st.Mode()&os.ModeSymlink != 0:
		linkTarget, err := os.Readlink(entry.current)
		if err != nil {
			return 0, err
		}
		if _, err := io.WriteString(w, linkTarget); err != nil {
			return 0, err
		}
	case st.Mode().IsRegular():
		f, err := os.Open(entry.current)
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
		return r.warnf(reporter, "zip create: unsupported local member type %s for %s; skipping payload", st.Mode().String(), entry.current), nil
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
