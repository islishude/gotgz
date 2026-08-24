package engine

import (
	"github.com/islishude/gotgz/packages/archivepath"
	"github.com/islishude/gotgz/packages/cli"
)

func loadReadExcludeMatcher(opts cli.Options) (*archivepath.CompiledPathMatcher, error) {
	if opts.Wildcards {
		if err := archivepath.ValidateGlobPatterns(opts.Members); err != nil {
			return nil, err
		}
	}
	patterns, err := archivepath.LoadExcludePatterns(opts.Exclude, opts.ExcludeFrom)
	if err != nil {
		return nil, err
	}
	return archivepath.NewCompiledPathMatcher(patterns), nil
}

func shouldSkipReadMember(memberMatcher, excludeMatcher *archivepath.CompiledPathMatcher, name string) bool {
	return archivepath.ShouldSkipMemberWithMatcher(memberMatcher, name) || archivepath.MatchExcludeWithMatcher(excludeMatcher, name)
}
