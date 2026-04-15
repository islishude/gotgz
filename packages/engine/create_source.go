package engine

import (
	"context"
	"fmt"
	"io/fs"
	"os"

	"github.com/islishude/gotgz/packages/archivepath"
	"github.com/islishude/gotgz/packages/cli"
	"github.com/islishude/gotgz/packages/locator"
)

// localCreateSource provides one local create workload as normalized archive
// records paired with filesystem metadata supplied by the implementation.
// Implementations may either refresh metadata immediately before calling the
// visitor or reuse metadata obtained during planning or traversal.
type localCreateSource interface {
	// Visit invokes visit for each record and its associated filesystem
	// metadata. Callers must not assume the metadata is freshly re-statted
	// unless the concrete implementation documents that guarantee.
	Visit(ctx context.Context, visit func(record localCreateRecord, info fs.FileInfo) error) error
}

// liveLocalCreateSource walks one local member directly from the filesystem.
type liveLocalCreateSource struct {
	member         string
	chdir          string
	excludeMatcher *archivepath.CompiledPathMatcher
}

// Visit streams one live local member walk to the supplied visitor.
func (s liveLocalCreateSource) Visit(ctx context.Context, visit func(record localCreateRecord, info fs.FileInfo) error) error {
	return walkLocalCreateMember(ctx, s.member, s.chdir, s.excludeMatcher, visit)
}

// plannedLocalCreateSource replays one pre-scanned local record list using
// fresh filesystem metadata at write time.
type plannedLocalCreateSource struct {
	records []localCreateRecord
}

// Visit replays one planned local member list while refreshing metadata for
// each record just before it is written.
func (s plannedLocalCreateSource) Visit(ctx context.Context, visit func(record localCreateRecord, info fs.FileInfo) error) error {
	for _, record := range s.records {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		info, err := os.Lstat(record.current)
		if err != nil {
			return err
		}
		if err := visit(record, info); err != nil {
			return err
		}
	}
	return nil
}

// createInputSource dispatches create-mode members and exposes any known total
// payload size for progress reporting.
// createInputSource abstracts the input used by a create operation.
//
// Implementations may represent local content, existing S3-backed content,
// or a combination of both. Total reports the overall payload size when it
// can be determined ahead of time. Visit walks the source and dispatches each
// encountered item to the appropriate handler, returning the number of visited
// items or the first error encountered.
type createInputSource interface {
	// Total returns the total payload size and whether that total is known upfront.
	Total() (int64, bool)
	// Visit walks the source and dispatches each item to the appropriate handler.
	Visit(ctx context.Context, handleS3 func(ref locator.Ref) error, handleLocal func(source localCreateSource) (int, error)) (int, error)
}

// liveCreateInputSource parses create members on demand without precomputing a plan.
type liveCreateInputSource struct {
	opts           cli.Options
	excludeMatcher *archivepath.CompiledPathMatcher
}

// Total reports that live create sources do not know their payload size upfront.
func (s liveCreateInputSource) Total() (int64, bool) {
	return 0, false
}

// Visit parses each create member and dispatches it by backend kind.
func (s liveCreateInputSource) Visit(ctx context.Context, handleS3 func(ref locator.Ref) error, handleLocal func(source localCreateSource) (int, error)) (int, error) {
	warnings := 0
	for _, member := range s.opts.Members {
		select {
		case <-ctx.Done():
			return warnings, ctx.Err()
		default:
		}

		ref, err := locator.ParseMember(member)
		if err != nil {
			return warnings, err
		}

		switch ref.Kind {
		case locator.KindS3:
			if archivepath.MatchExcludeWithMatcher(s.excludeMatcher, ref.Key) {
				continue
			}
			if err := handleS3(ref); err != nil {
				return warnings, err
			}
		case locator.KindLocal:
			w, err := handleLocal(liveLocalCreateSource{
				member:         member,
				chdir:          s.opts.Chdir,
				excludeMatcher: s.excludeMatcher,
			})
			warnings += w
			if err != nil {
				return warnings, err
			}
		default:
			return warnings, fmt.Errorf("unsupported member reference %q", member)
		}
	}
	return warnings, nil
}

// plannedCreateInputSource replays one pre-scanned create plan.
type plannedCreateInputSource struct {
	plan *createPlan
}

// Total reports the pre-scanned payload size estimate attached to the plan.
func (s plannedCreateInputSource) Total() (int64, bool) {
	return s.plan.totalBytes, s.plan.totalKnown
}

// Visit dispatches each pre-scanned create member without rescanning local trees.
func (s plannedCreateInputSource) Visit(ctx context.Context, handleS3 func(ref locator.Ref) error, handleLocal func(source localCreateSource) (int, error)) (int, error) {
	warnings := 0
	for _, member := range s.plan.members {
		select {
		case <-ctx.Done():
			return warnings, ctx.Err()
		default:
		}

		switch member.ref.Kind {
		case locator.KindS3:
			if err := handleS3(member.ref); err != nil {
				return warnings, err
			}
		case locator.KindLocal:
			w, err := handleLocal(plannedLocalCreateSource{records: member.localRecords})
			warnings += w
			if err != nil {
				return warnings, err
			}
		}
	}
	return warnings, nil
}

// newCreateInputSource chooses a live or pre-scanned create source depending on
// whether the caller needs an upfront total for progress reporting.
func (r *Runner) newCreateInputSource(ctx context.Context, opts cli.Options, excludeMatcher *archivepath.CompiledPathMatcher, precomputeTotal bool) (createInputSource, error) {
	if !precomputeTotal {
		return liveCreateInputSource{
			opts:           opts,
			excludeMatcher: excludeMatcher,
		}, nil
	}

	plan, err := r.buildCreatePlan(ctx, opts, excludeMatcher)
	if err != nil {
		return nil, err
	}
	return plannedCreateInputSource{plan: plan}, nil
}

// visitLocalCreateSource consumes one local create source and accumulates any
// warnings reported by the caller-supplied entry handler.
func visitLocalCreateSource(ctx context.Context, source localCreateSource, handle func(record localCreateRecord, info fs.FileInfo) (int, error)) (int, error) {
	warnings := 0
	err := source.Visit(ctx, func(record localCreateRecord, info fs.FileInfo) error {
		w, err := handle(record, info)
		warnings += w
		return err
	})
	return warnings, err
}
