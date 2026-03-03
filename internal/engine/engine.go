package engine

import (
	"archive/tar"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/islishude/gotgz/internal/archive"
	"github.com/islishude/gotgz/internal/cli"
	"github.com/islishude/gotgz/internal/compress"
	"github.com/islishude/gotgz/internal/locator"
	httpstore "github.com/islishude/gotgz/internal/storage/http"
	localstore "github.com/islishude/gotgz/internal/storage/local"
	s3store "github.com/islishude/gotgz/internal/storage/s3"
)

const (
	ExitSuccess = 0
	ExitWarning = 1
	ExitFatal   = 2
)

type PermissionPolicy struct {
	SameOwner    bool
	SamePerms    bool
	NumericOwner bool
}

type MetadataPolicy struct {
	Xattrs bool
	ACL    bool
}

type Runner struct {
	local  *localstore.ArchiveStore
	s3     *s3store.Store
	http   *httpstore.Store
	stderr io.Writer
	stdout io.Writer
}

type RunResult struct {
	ExitCode int
	Err      error
}

// archiveReaderInfo holds metadata returned alongside an opened archive reader.
type archiveReaderInfo struct {
	Size        int64
	SizeKnown   bool
	ContentType string
}

func New(ctx context.Context, stdout io.Writer, stderr io.Writer) (*Runner, error) {
	s3s, err := s3store.New(ctx)
	if err != nil {
		return nil, fmt.Errorf("init s3: %w", err)
	}
	return &Runner{
		local:  &localstore.ArchiveStore{},
		s3:     s3s,
		http:   httpstore.New(),
		stdout: stdout,
		stderr: stderr,
	}, nil
}

func (r *Runner) Run(ctx context.Context, opts cli.Options) RunResult {
	switch opts.Mode {
	case cli.ModeCreate:
		warnings, err := r.runCreate(ctx, opts)
		return classifyResult(err, warnings)
	case cli.ModeExtract:
		warnings, err := r.runExtract(ctx, opts)
		return classifyResult(err, warnings)
	case cli.ModeList:
		warnings, err := r.runList(ctx, opts)
		return classifyResult(err, warnings)
	default:
		return RunResult{ExitCode: ExitFatal, Err: fmt.Errorf("unsupported mode %q", opts.Mode)}
	}
}

func classifyResult(err error, warnings int) RunResult {
	if err != nil {
		return RunResult{ExitCode: ExitFatal, Err: err}
	}
	if warnings > 0 {
		return RunResult{ExitCode: ExitWarning}
	}
	return RunResult{ExitCode: ExitSuccess}
}

func (r *Runner) runCreate(ctx context.Context, opts cli.Options) (warnings int, retErr error) {
	archiveRef, err := locator.ParseArchive(opts.Archive)
	if err != nil {
		return 0, err
	}
	format := detectCreateArchiveFormat(archiveRef)
	switch format {
	case archiveFormatZip:
		return r.runCreateZip(ctx, opts, archiveRef)
	default:
		return r.runCreateTar(ctx, opts, archiveRef)
	}
}

// runCreateTar writes create-mode output in tar format.
func (r *Runner) runCreateTar(ctx context.Context, opts cli.Options, archiveRef locator.Ref) (warnings int, retErr error) {
	metadataPolicy := resolveMetadataPolicy(opts)
	reporter := newProgressReporter(r.stderr, opts.Progress, 0, false, time.Now(), opts.Verbose)
	defer reporter.Finish()

	if opts.Suffix != "" {
		switch archiveRef.Kind {
		case locator.KindLocal:
			archiveRef.Path = AddTarSuffix(archiveRef.Path, opts.Suffix)
			archiveRef.Raw = archiveRef.Path
		case locator.KindS3:
			archiveRef.Key = AddTarSuffix(archiveRef.Key, opts.Suffix)
		case locator.KindStdio:
			return 0, fmt.Errorf("cannot use -suffix with -f -")
		}
	}
	aw, err := r.openArchiveWriter(ctx, archiveRef)
	if err != nil {
		return 0, err
	}

	cw, err := compress.NewWriter(aw, compress.FromString(string(opts.Compression)), compress.WriterOptions{Level: opts.CompressionLevel})
	if err != nil {
		_ = aw.Close()
		return 0, err
	}
	defer func() {
		// cw.Close() also closes the underlying archive writer.
		if cerr := cw.Close(); cerr != nil && retErr == nil {
			retErr = fmt.Errorf("closing archive: %w", cerr)
		}
	}()

	tw := tar.NewWriter(cw)
	defer func() {
		if cerr := tw.Close(); cerr != nil && retErr == nil {
			retErr = fmt.Errorf("closing tar writer: %w", cerr)
		}
	}()

	excludes, err := loadExcludePatterns(opts.Exclude, opts.ExcludeFrom)
	if err != nil {
		return 0, err
	}
	totalBytes, known, err := r.estimateCreateInputBytes(ctx, opts, excludes)
	if err != nil {
		return 0, err
	}
	reporter.SetTotal(totalBytes, known)

	for _, m := range opts.Members {
		select {
		case <-ctx.Done():
			return warnings, ctx.Err()
		default:
		}
		ref, err := locator.ParseMember(m)
		if err != nil {
			return warnings, err
		}
		switch ref.Kind {
		case locator.KindS3:
			if matchExclude(excludes, ref.Key) {
				continue
			}
			if err := r.addS3Member(ctx, tw, ref, opts.Verbose, reporter); err != nil {
				return warnings, err
			}
		case locator.KindLocal:
			if err := r.addLocalPath(ctx, tw, m, opts.Chdir, excludes, opts.Verbose, metadataPolicy, reporter); err != nil {
				return warnings, err
			}
		default:
			return warnings, fmt.Errorf("unsupported member reference %q", m)
		}
	}
	return warnings, nil
}

// estimateCreateInputBytes pre-computes input bytes for create mode progress and ETA.
func (r *Runner) estimateCreateInputBytes(ctx context.Context, opts cli.Options, excludes []string) (int64, bool, error) {
	var total int64
	for _, member := range opts.Members {
		select {
		case <-ctx.Done():
			return 0, false, ctx.Err()
		default:
		}

		ref, err := locator.ParseMember(member)
		if err != nil {
			return 0, false, err
		}

		switch ref.Kind {
		case locator.KindS3:
			if matchExclude(excludes, ref.Key) {
				continue
			}
			meta, err := r.s3.Stat(ctx, ref)
			if err != nil {
				// Cannot determine size for this S3 object; fall back to
				// unknown-total progress so the actual archive still proceeds.
				return total, false, nil
			}
			total += meta.Size
		case locator.KindLocal:
			size, err := r.estimateLocalPathBytes(ctx, member, opts.Chdir, excludes)
			if err != nil {
				return 0, false, err
			}
			total += size
		default:
			return 0, false, fmt.Errorf("unsupported member reference %q", member)
		}
	}
	return total, true, nil
}

// estimateLocalPathBytes sums regular file bytes for one local create member.
func (r *Runner) estimateLocalPathBytes(ctx context.Context, member, chdir string, excludes []string) (int64, error) {
	basePath := member
	if chdir != "" {
		basePath = filepath.Join(chdir, member)
	}
	cleanMember := path.Clean(filepath.ToSlash(member))
	var total int64

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
		if st.Mode().IsRegular() {
			total += st.Size()
		}
		return nil
	})
	return total, err
}

func (r *Runner) addS3Member(ctx context.Context, tw *tar.Writer, ref locator.Ref, verbose bool, reporter *progressReporter) (err error) {
	if strings.TrimSpace(ref.Key) == "" {
		return fmt.Errorf("s3 member key cannot be empty: %q", ref.Raw)
	}
	body, meta, err := r.s3.OpenReader(ctx, ref)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := body.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()

	hdr := &tar.Header{
		Name:     ref.Key,
		Mode:     0o644,
		Size:     meta.Size,
		Typeflag: tar.TypeReg,
		ModTime:  time.Now(),
		Format:   tar.FormatPAX,
	}
	if err := tw.WriteHeader(hdr); err != nil {
		return err
	}
	if _, err := io.Copy(tw, newCountingReader(body, reporter)); err != nil {
		return err
	}
	if verbose {
		reporter.beforeExternalLineOutput()
		_, _ = fmt.Fprintln(r.stdout, hdr.Name)
		reporter.afterExternalLineOutput()
	}
	return nil
}

func (r *Runner) addLocalPath(ctx context.Context, tw *tar.Writer, member, chdir string, excludes []string, verbose bool, metadataPolicy MetadataPolicy, reporter *progressReporter) error {
	basePath := member
	if chdir != "" {
		basePath = filepath.Join(chdir, member)
	}
	cleanMember := path.Clean(filepath.ToSlash(member))
	return filepath.WalkDir(basePath, func(current string, d fs.DirEntry, walkErr error) error {
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
		linkname := ""
		if st.Mode()&os.ModeSymlink != 0 {
			linkname, _ = os.Readlink(current)
		}
		hdr, err := tar.FileInfoHeader(st, linkname)
		if err != nil {
			return err
		}
		hdr.Name = filepath.ToSlash(archiveName)
		hdr.Format = tar.FormatPAX

		if metadataPolicy.Xattrs || metadataPolicy.ACL {
			xattrs, acls, _ := archive.ReadPathMetadata(current)
			xattrs, acls = prepareMetadataForArchive(xattrs, acls, metadataPolicy)
			archive.EncodeXattrToPAX(hdr, xattrs)
			archive.EncodeACLToPAX(hdr, acls)
		}

		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		if st.Mode().IsRegular() {
			f, err := os.Open(current)
			if err != nil {
				return err
			}
			_, err = io.Copy(tw, newCountingReader(f, reporter))
			cerr := f.Close()
			if err != nil {
				return err
			}
			if cerr != nil {
				return cerr
			}
		}
		if verbose {
			reporter.beforeExternalLineOutput()
			_, _ = fmt.Fprintln(r.stdout, hdr.Name)
			reporter.afterExternalLineOutput()
		}
		return nil
	})
}

func (r *Runner) runList(ctx context.Context, opts cli.Options) (int, error) {
	reporter := newProgressReporter(r.stderr, opts.Progress, 0, false, time.Now(), true)
	defer reporter.Finish()
	ref, ar, info, magic, err := r.openArchiveForRead(ctx, opts.Archive)
	if err != nil {
		return 0, err
	}
	defer ar.Close() //nolint:errcheck

	switch detectReadArchiveFormat(magic, archiveNameHint(ref), info.ContentType) {
	case archiveFormatZip:
		return r.runListZip(ctx, opts, reporter, ref, ar, info)
	default:
		return r.runListTar(ctx, opts, reporter, ar, info)
	}
}

func (r *Runner) runExtract(ctx context.Context, opts cli.Options) (int, error) {
	reporter := newProgressReporter(r.stderr, opts.Progress, 0, false, time.Now(), opts.Verbose && !opts.ToStdout)
	defer reporter.Finish()
	ref, ar, info, magic, err := r.openArchiveForRead(ctx, opts.Archive)
	if err != nil {
		return 0, err
	}
	defer ar.Close() //nolint:errcheck

	switch detectReadArchiveFormat(magic, archiveNameHint(ref), info.ContentType) {
	case archiveFormatZip:
		return r.runExtractZip(ctx, opts, reporter, ref, ar, info)
	default:
		return r.runExtractTar(ctx, opts, reporter, ar, info)
	}
}

// runListTar lists archive members from a tar input stream.
func (r *Runner) runListTar(ctx context.Context, opts cli.Options, reporter *progressReporter, ar io.ReadCloser, info archiveReaderInfo) (int, error) {
	return r.scanTarArchiveFromReader(opts, reporter, info, ar, func(hdr *tar.Header, tr *tar.Reader) (int, error) {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		default:
		}
		if shouldSkipMember(opts, hdr.Name) {
			if _, err := io.Copy(io.Discard, tr); err != nil {
				return 0, err
			}
			return 0, nil
		}
		reporter.beforeExternalLineOutput()
		_, _ = fmt.Fprintln(r.stdout, hdr.Name)
		reporter.afterExternalLineOutput()
		if _, err := io.Copy(io.Discard, tr); err != nil {
			return 0, err
		}
		return 0, nil
	})
}

// runExtractTar extracts archive members from a tar input stream.
func (r *Runner) runExtractTar(ctx context.Context, opts cli.Options, reporter *progressReporter, ar io.ReadCloser, info archiveReaderInfo) (int, error) {
	policy := resolvePolicy(opts)
	metadataPolicy := resolveMetadataPolicy(opts)

	if opts.ToStdout {
		return r.scanTarArchiveFromReader(opts, reporter, info, ar, func(hdr *tar.Header, tr *tar.Reader) (int, error) {
			if shouldSkipMember(opts, hdr.Name) {
				if _, err := io.Copy(io.Discard, tr); err != nil {
					return 0, err
				}
				return 0, nil
			}
			if _, ok := stripPathComponents(hdr.Name, opts.StripComponents); !ok {
				if _, err := io.Copy(io.Discard, io.LimitReader(tr, hdr.Size)); err != nil {
					return 0, err
				}
				return 0, nil
			}
			if hdr.Typeflag != tar.TypeReg {
				if _, err := io.Copy(io.Discard, tr); err != nil {
					return 0, err
				}
				return 0, nil
			}
			_, err := io.Copy(r.stdout, tr)
			return 0, err
		})
	}

	target := opts.Chdir
	if target == "" {
		target = "."
	}
	parsedTarget, err := locator.ParseArchive(target)
	if err != nil {
		return 0, err
	}

	return r.scanTarArchiveFromReader(opts, reporter, info, ar, func(hdr *tar.Header, tr *tar.Reader) (int, error) {
		if shouldSkipMember(opts, hdr.Name) {
			if _, err := io.Copy(io.Discard, tr); err != nil {
				return 0, err
			}
			return 0, nil
		}
		extractName, ok := stripPathComponents(hdr.Name, opts.StripComponents)
		if !ok {
			if _, err := io.Copy(io.Discard, io.LimitReader(tr, hdr.Size)); err != nil {
				return 0, err
			}
			return 0, nil
		}
		effectiveHdr := *hdr
		effectiveHdr.Name = extractName
		if opts.Verbose {
			reporter.beforeExternalLineOutput()
			_, _ = fmt.Fprintln(r.stdout, effectiveHdr.Name)
			reporter.afterExternalLineOutput()
		}
		switch parsedTarget.Kind {
		case locator.KindS3:
			return r.extractToS3(ctx, parsedTarget, &effectiveHdr, tr, reporter)
		case locator.KindLocal, locator.KindStdio:
			return r.extractToLocal(parsedTarget.Path, &effectiveHdr, tr, policy, metadataPolicy)
		default:
			return 0, fmt.Errorf("unsupported extract target %q", target)
		}
	})
}

func (r *Runner) extractToS3(ctx context.Context, target locator.Ref, hdr *tar.Header, tr *tar.Reader, reporter *progressReporter) (int, error) {
	warnings := 0
	name := strings.TrimPrefix(hdr.Name, "./")
	if name == "" {
		if hdr.Size > 0 {
			if _, err := io.Copy(io.Discard, io.LimitReader(tr, hdr.Size)); err != nil {
				return warnings, err
			}
		}
		// Do not create an S3 object when the entry name is empty.
		return warnings, nil
	}
	obj := locator.Ref{Kind: locator.KindS3, Bucket: target.Bucket, Key: locator.JoinS3Prefix(target.Key, name)}
	meta, ok := archive.HeaderToS3Metadata(hdr)
	meta = mergeMetadata(target.Metadata, meta)
	if !ok {
		warnings++
		reporter.beforeExternalLineOutput()
		_, _ = fmt.Fprintf(r.stderr, "gotgz: warning: metadata exceeds S3 metadata limit for %s\n", hdr.Name)
		reporter.afterExternalLineOutput()
	}

	switch hdr.Typeflag {
	case tar.TypeReg:
		if err := r.s3.UploadStream(ctx, obj, io.LimitReader(tr, hdr.Size), meta); err != nil {
			return warnings, err
		}
	case tar.TypeDir:
		// S3 has no real directories. Still need to consume any data associated with this entry.
		if _, err := io.Copy(io.Discard, io.LimitReader(tr, hdr.Size)); err != nil {
			return warnings, err
		}
	default:
		empty := strings.NewReader("")
		meta["gotgz-type"] = fmt.Sprintf("%d", hdr.Typeflag)
		if err := r.s3.UploadStream(ctx, obj, empty, meta); err != nil {
			return warnings, err
		}
	}
	return warnings, nil
}

func (r *Runner) extractToLocal(base string, hdr *tar.Header, tr *tar.Reader, policy PermissionPolicy, metadataPolicy MetadataPolicy) (int, error) {
	target, err := safeJoin(base, hdr.Name)
	if err != nil {
		return 0, err
	}
	mode := fs.FileMode(hdr.Mode)
	if !policy.SamePerms {
		mode = mode &^ currentUmask()
	}

	switch hdr.Typeflag {
	case tar.TypeDir:
		if err := os.MkdirAll(target, mode.Perm()); err != nil {
			return 0, err
		}
	case tar.TypeReg:
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return 0, err
		}
		f, err := os.OpenFile(target, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, mode.Perm())
		if err != nil {
			return 0, err
		}
		_, err = io.Copy(f, io.LimitReader(tr, hdr.Size))
		cerr := f.Close()
		if err != nil {
			return 0, err
		}
		if cerr != nil {
			return 0, cerr
		}
	case tar.TypeSymlink:
		if err := safeSymlinkTarget(base, target, hdr.Linkname); err != nil {
			return 0, err
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return 0, err
		}
		if err := os.Remove(target); err != nil && !errors.Is(err, os.ErrNotExist) {
			return 0, err
		}
		if err := os.Symlink(hdr.Linkname, target); err != nil {
			return 0, err
		}
	case tar.TypeLink:
		linkTarget, err := safeJoin(base, hdr.Linkname)
		if err != nil {
			return 0, err
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return 0, err
		}
		_ = os.Remove(target)
		if err := os.Link(linkTarget, target); err != nil {
			return 0, err
		}
	default:
		if _, err := io.Copy(io.Discard, io.LimitReader(tr, hdr.Size)); err != nil {
			return 0, err
		}
		return 0, nil
	}

	if policy.SamePerms {
		_ = os.Chmod(target, fs.FileMode(hdr.Mode).Perm())
	}
	if policy.SameOwner {
		_ = os.Lchown(target, hdr.Uid, hdr.Gid)
	}
	if !hdr.ModTime.IsZero() {
		_ = os.Chtimes(target, hdr.ModTime, hdr.ModTime)
	}

	xattrs, acls := decodeMetadataForExtract(hdr, metadataPolicy)
	_ = archive.WritePathMetadata(target, xattrs, acls)
	return 0, nil
}

// scanTarArchiveFromReader scans a tar stream with optional compression.
func (r *Runner) scanTarArchiveFromReader(opts cli.Options, reporter *progressReporter, info archiveReaderInfo, ar io.ReadCloser, fn func(hdr *tar.Header, tr *tar.Reader) (int, error)) (int, error) {
	reporter.SetTotal(info.Size, info.SizeKnown)
	ar = newCountingReadCloser(ar, reporter)

	cr, _, err := compress.NewReader(ar, compress.FromString(string(opts.Compression)), opts.Archive, info.ContentType)
	if err != nil {
		return 0, err
	}
	defer cr.Close() //nolint:errcheck

	tr := tar.NewReader(cr)
	warnings := 0
	for {
		hdr, err := tr.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return warnings, err
		}
		w, err := fn(hdr, tr)
		warnings += w
		if err != nil {
			return warnings, err
		}
	}
	return warnings, nil
}

// openArchiveForRead opens a readable archive stream and returns the stream
// plus a replayable magic-byte prefix used for archive format detection.
func (r *Runner) openArchiveForRead(ctx context.Context, archive string) (locator.Ref, io.ReadCloser, archiveReaderInfo, []byte, error) {
	ref, err := locator.ParseArchive(archive)
	if err != nil {
		return locator.Ref{}, nil, archiveReaderInfo{}, nil, err
	}
	ar, info, err := r.openArchiveReader(ctx, ref)
	if err != nil {
		return locator.Ref{}, nil, archiveReaderInfo{}, nil, err
	}
	magic, replay, err := replayWithMagicPrefix(ar, 8)
	if err != nil {
		_ = ar.Close()
		return locator.Ref{}, nil, archiveReaderInfo{}, nil, err
	}
	return ref, replay, info, magic, nil
}

// openArchiveReader opens the archive for reading and returns the reader along
// with metadata about the archive (size, whether the size is known, and the
// content type).
func (r *Runner) openArchiveReader(ctx context.Context, ref locator.Ref) (io.ReadCloser, archiveReaderInfo, error) {
	switch ref.Kind {
	case locator.KindLocal, locator.KindStdio:
		rc, meta, err := r.local.OpenReader(ref)
		if err != nil {
			return nil, archiveReaderInfo{}, err
		}
		return rc, archiveReaderInfo{Size: meta.Size, SizeKnown: ref.Kind == locator.KindLocal}, nil
	case locator.KindS3:
		if strings.TrimSpace(ref.Key) == "" {
			return nil, archiveReaderInfo{}, fmt.Errorf("archive object key cannot be empty for -f")
		}
		rc, meta, err := r.s3.OpenReader(ctx, ref)
		if err != nil {
			return nil, archiveReaderInfo{}, err
		}
		return rc, archiveReaderInfo{Size: meta.Size, SizeKnown: true, ContentType: strings.TrimSpace(meta.ContentType)}, nil
	case locator.KindHTTP:
		rc, meta, err := r.http.OpenReader(ctx, ref)
		if err != nil {
			return nil, archiveReaderInfo{}, err
		}
		if meta.Size >= 0 {
			return rc, archiveReaderInfo{Size: meta.Size, SizeKnown: true, ContentType: meta.ContentType}, nil
		}
		return rc, archiveReaderInfo{ContentType: meta.ContentType}, nil
	default:
		return nil, archiveReaderInfo{}, fmt.Errorf("unsupported archive source %q", ref.Raw)
	}
}

func (r *Runner) openArchiveWriter(ctx context.Context, ref locator.Ref) (io.WriteCloser, error) {
	switch ref.Kind {
	case locator.KindLocal, locator.KindStdio:
		return r.local.OpenWriter(ref)
	case locator.KindS3:
		if strings.TrimSpace(ref.Key) == "" {
			return nil, fmt.Errorf("archive object key cannot be empty for -f")
		}
		return r.s3.OpenWriter(ctx, ref, ref.Metadata)
	case locator.KindHTTP:
		return nil, fmt.Errorf("unsupported archive target %q: http(s) archives are source-only", ref.Raw)
	default:
		return nil, fmt.Errorf("unsupported archive target %q", ref.Raw)
	}
}

func mergeMetadata(base, overlay map[string]string) map[string]string {
	if len(base) == 0 && len(overlay) == 0 {
		return nil
	}
	out := make(map[string]string, len(base)+len(overlay))
	maps.Copy(out, base)
	maps.Copy(out, overlay)
	return out
}

func resolvePolicy(opts cli.Options) PermissionPolicy {
	isRoot := os.Geteuid() == 0
	policy := PermissionPolicy{SameOwner: isRoot, SamePerms: isRoot, NumericOwner: opts.NumericOwner}
	if opts.SameOwner != nil {
		policy.SameOwner = *opts.SameOwner
	}
	if opts.SamePermissions != nil {
		policy.SamePerms = *opts.SamePermissions
	}
	return policy
}

func resolveMetadataPolicy(opts cli.Options) MetadataPolicy {
	return MetadataPolicy{
		Xattrs: opts.Xattrs,
		ACL:    opts.ACL,
	}
}

func shouldSkipMember(opts cli.Options, name string) bool {
	if len(opts.Members) == 0 {
		return false
	}
	for _, m := range opts.Members {
		if opts.Wildcards {
			ok, _ := path.Match(m, name)
			if ok {
				return false
			}
			continue
		}
		if m == name {
			return false
		}
	}
	return true
}

func loadExcludePatterns(inline []string, files []string) ([]string, error) {
	out := make([]string, 0, len(inline))
	for _, pattern := range inline {
		if _, err := path.Match(pattern, ""); err != nil {
			return nil, fmt.Errorf("invalid exclude pattern %q: %w", pattern, err)
		}
		out = append(out, pattern)
	}
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			return nil, err
		}
		lineNo := 0
		for line := range strings.SplitSeq(string(b), "\n") {
			lineNo++
			line = strings.TrimSpace(line)
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			if _, err := path.Match(line, ""); err != nil {
				return nil, fmt.Errorf("invalid exclude pattern %q in %s:%d: %w", line, f, lineNo, err)
			}
			out = append(out, line)
		}
	}
	return out, nil
}

func matchExclude(patterns []string, name string) bool {
	for _, p := range patterns {
		if ok, _ := path.Match(p, name); ok {
			return true
		}
	}
	return false
}

// safeSymlinkTarget validates that a symlink's target does not escape the
// extraction base directory. linkname is the raw target from the archive;
// symlinkPath is the absolute path where the symlink will be created.
func safeSymlinkTarget(base, symlinkPath, linkname string) error {
	if linkname == "" {
		return fmt.Errorf("symlink target is empty")
	}
	base = filepath.Clean(base)

	var resolved string
	if filepath.IsAbs(linkname) {
		// Absolute symlink targets are resolved within the base directory.
		resolved = filepath.Join(base, filepath.FromSlash(linkname))
	} else {
		// Relative symlink targets are resolved from the symlink's parent.
		resolved = filepath.Join(filepath.Dir(symlinkPath), filepath.FromSlash(linkname))
	}
	resolved = filepath.Clean(resolved)

	rel, err := filepath.Rel(base, resolved)
	if err != nil {
		return fmt.Errorf("refusing symlink: cannot compute relative path: %w", err)
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return fmt.Errorf("refusing symlink %q -> %q: target escapes extraction directory", symlinkPath, linkname)
	}
	return nil
}

func safeJoin(base, member string) (string, error) {
	base = filepath.Clean(base)
	member = strings.TrimPrefix(member, "/")
	candidate := filepath.Join(base, filepath.FromSlash(member))
	candidate = filepath.Clean(candidate)
	rel, err := filepath.Rel(base, candidate)
	if err != nil {
		return "", err
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("refusing to write outside target directory: %s", member)
	}
	return candidate, nil
}

func stripPathComponents(name string, count int) (string, bool) {
	if count <= 0 {
		return name, true
	}
	clean := path.Clean(strings.TrimPrefix(name, "/"))
	parts := make([]string, 0)
	for p := range strings.SplitSeq(clean, "/") {
		if p == "" || p == "." {
			continue
		}
		parts = append(parts, p)
	}
	if len(parts) <= count {
		return "", false
	}
	return strings.Join(parts[count:], "/"), true
}

func filterACLLikeXattrs(attrs map[string][]byte) map[string][]byte {
	if len(attrs) == 0 {
		return attrs
	}
	out := make(map[string][]byte, len(attrs))
	for k, v := range attrs {
		if strings.Contains(strings.ToLower(k), "acl") {
			continue
		}
		out[k] = v
	}
	return out
}

func prepareMetadataForArchive(xattrs map[string][]byte, acls map[string][]byte, policy MetadataPolicy) (map[string][]byte, map[string][]byte) {
	if !policy.Xattrs {
		xattrs = nil
	} else if !policy.ACL {
		xattrs = filterACLLikeXattrs(xattrs)
	}
	if !policy.ACL {
		acls = nil
	}
	return xattrs, acls
}

func decodeMetadataForExtract(hdr *tar.Header, policy MetadataPolicy) (map[string][]byte, map[string][]byte) {
	var xattrs map[string][]byte
	if policy.Xattrs {
		xattrs, _ = archive.DecodeXattrFromPAX(hdr)
		if !policy.ACL {
			xattrs = filterACLLikeXattrs(xattrs)
		}
	}

	var acls map[string][]byte
	if policy.ACL {
		acls, _ = archive.DecodeACLFromPAX(hdr)
	}
	return xattrs, acls
}
