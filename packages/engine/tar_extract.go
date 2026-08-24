package engine

import (
	"archive/tar"
	"context"
	"errors"
	"io"

	"github.com/islishude/gotgz/packages/archivepath"
	"github.com/islishude/gotgz/packages/archiveprogress"
	"github.com/islishude/gotgz/packages/archiveutil"
	"github.com/islishude/gotgz/packages/cli"
	"github.com/islishude/gotgz/packages/locator"
)

type tarExtractState struct {
	policy          PermissionPolicy
	metadataPolicy  MetadataPolicy
	memberMatcher   *archivepath.CompiledPathMatcher
	excludeMatcher  *archivepath.CompiledPathMatcher
	parsedTarget    locator.Ref
	target          string
	safetyCache     *archivepath.PathSafetyCache
	metadataSession *localMetadataSession
	baseWarnings    int
}

// runExtractTar extracts archive members from a tar input stream or split-volume set.
func (r *Runner) runExtractTar(ctx context.Context, opts cli.Options, reporter *archiveprogress.Reporter, ref locator.Ref, ar io.ReadCloser, info archiveReaderInfo, excludeMatcher *archivepath.CompiledPathMatcher) (int, error) {
	state, err := r.prepareTarExtractState(opts, reporter, excludeMatcher)
	if err != nil {
		return 0, err
	}
	volumes, err := r.resolveArchiveVolumes(ctx, ref, info)
	if err != nil {
		return 0, err
	}

	scan := func(scanReader io.ReadCloser, scanInfo archiveReaderInfo) (int, error) {
		return r.runExtractTarReader(ctx, opts, reporter, scanReader, scanInfo, state)
	}

	var warnings int
	if len(volumes) == 1 {
		reporter.SetTotal(info.Size, info.SizeKnown)
		warnings, err = scan(ar, info)
	} else {
		warnings, err = r.scanTarArchiveFromVolumes(ctx, opts, reporter, volumes, ar, scan)
	}
	metadataWarnings, metadataErr := state.metadataSession.finish()
	warnings += state.baseWarnings + metadataWarnings
	return warnings, errors.Join(err, metadataErr)
}

func (r *Runner) prepareTarExtractState(opts cli.Options, reporter *archiveprogress.Reporter, excludeMatcher *archivepath.CompiledPathMatcher) (*tarExtractState, error) {
	metadataPolicy, baseWarnings := r.effectiveMetadataPolicy(opts, reporter)
	state := &tarExtractState{
		policy:         opts.ResolvePermissionPolicy(),
		metadataPolicy: metadataPolicy,
		memberMatcher:  archivepath.NewMemberMatcher(opts.Members, opts.Wildcards),
		excludeMatcher: excludeMatcher,
		baseWarnings:   baseWarnings,
	}
	if opts.ToStdout {
		return state, nil
	}
	parsedTarget, err := locator.ParseExtractTarget(opts.Chdir, opts.S3CacheControl, opts.S3ObjectTags)
	if err != nil {
		return nil, err
	}
	state.parsedTarget = parsedTarget
	state.target = opts.Chdir
	if state.target == "" {
		state.target = "."
	}
	if parsedTarget.Kind == locator.KindLocal || parsedTarget.Kind == locator.KindStdio {
		state.safetyCache = archivepath.NewPathSafetyCache()
		state.metadataSession = newLocalMetadataSession(r, reporter)
	}
	return state, nil
}

// runExtractTarReader extracts archive members from a single tar volume reader.
func (r *Runner) runExtractTarReader(ctx context.Context, opts cli.Options, reporter *archiveprogress.Reporter, ar io.ReadCloser, info archiveReaderInfo, state *tarExtractState) (int, error) {
	if opts.ToStdout {
		return r.scanTarArchiveFromReader(ctx, opts, reporter, info, opts.Archive, ar, func(hdr *tar.Header, tr *tar.Reader) (int, error) {
			if shouldSkipReadMember(state.memberMatcher, state.excludeMatcher, hdr.Name) {
				if _, err := archiveutil.CopyWithContext(ctx, io.Discard, tr); err != nil {
					return 0, err
				}
				return 0, nil
			}
			if _, ok := archivepath.StripPathComponents(hdr.Name, opts.StripComponents); !ok {
				if _, err := archiveutil.CopyWithContext(ctx, io.Discard, io.LimitReader(tr, hdr.Size)); err != nil {
					return 0, err
				}
				return 0, nil
			}
			if hdr.Typeflag != tar.TypeReg {
				if _, err := archiveutil.CopyWithContext(ctx, io.Discard, tr); err != nil {
					return 0, err
				}
				return 0, nil
			}
			_, err := archiveutil.CopyWithContext(ctx, r.stdout, tr)
			return 0, err
		})
	}

	return r.scanTarArchiveFromReader(ctx, opts, reporter, info, opts.Archive, ar, func(hdr *tar.Header, tr *tar.Reader) (int, error) {
		if shouldSkipReadMember(state.memberMatcher, state.excludeMatcher, hdr.Name) {
			if _, err := archiveutil.CopyWithContext(ctx, io.Discard, tr); err != nil {
				return 0, err
			}
			return 0, nil
		}
		extractName, ok := archivepath.StripPathComponents(hdr.Name, opts.StripComponents)
		if !ok {
			if _, err := archiveutil.CopyWithContext(ctx, io.Discard, io.LimitReader(tr, hdr.Size)); err != nil {
				return 0, err
			}
			return 0, nil
		}
		effectiveHdr := *hdr
		effectiveHdr.Name = extractName
		if effectiveHdr.Typeflag == tar.TypeLink {
			linkName, linkOK := archivepath.StripPathComponents(effectiveHdr.Linkname, opts.StripComponents)
			if !linkOK || linkName == "" {
				if _, err := archiveutil.CopyWithContext(ctx, io.Discard, io.LimitReader(tr, hdr.Size)); err != nil {
					return 0, err
				}
				return r.warnf(reporter, "hardlink %s target %s was removed by --strip-components; skipping", hdr.Name, hdr.Linkname), nil
			}
			effectiveHdr.Linkname = linkName
		}
		if opts.Verbose {
			reporter.ExternalLinef(r.stdout, "%s\n", effectiveHdr.Name)
		}
		return r.dispatchExtractTarget(
			state.parsedTarget,
			state.target,
			func(target locator.Ref) (int, error) {
				return r.extractToS3(ctx, target, &effectiveHdr, tr, reporter)
			},
			func(base string) (int, error) {
				return r.extractToLocal(ctx, base, &effectiveHdr, tr, state.policy, state.metadataPolicy, state.safetyCache, state.metadataSession, reporter)
			},
		)
	})
}
