package engine

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/islishude/gotgz/packages/cli"
	"github.com/islishude/gotgz/packages/locator"
	localstore "github.com/islishude/gotgz/packages/storage/local"
)

func TestSelectCreateStrategyUsesCapabilitiesAndInputScope(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "input"), []byte("payload"), 0o600); err != nil {
		t.Fatalf("WriteFile(input) error = %v", err)
	}
	archivePath := filepath.Join(root, "archive.tar")
	localRef := locator.Ref{Kind: locator.KindLocal, Raw: archivePath, Path: archivePath}
	transactional := newRunner(&localstore.ArchiveStore{}, nil, nil, io.Discard, io.Discard)
	legacy := newRunner(fakeLocalArchiveStore{}, nil, nil, io.Discard, io.Discard)

	tests := []struct {
		name   string
		runner *Runner
		opts   cli.Options
		ref    locator.Ref
		want   createStrategy
	}{
		{
			name:   "transactional local single file streams",
			runner: transactional,
			opts:   cli.Options{Archive: archivePath, Chdir: root, Members: []string{"input"}},
			ref:    localRef,
			want:   createStrategyStreamingPlan,
		},
		{
			name:   "legacy local writer uses full plan",
			runner: legacy,
			opts:   cli.Options{Archive: archivePath, Chdir: root, Members: []string{"input"}},
			ref:    localRef,
			want:   createStrategyFullPlan,
		},
		{
			name:   "split output uses full plan",
			runner: transactional,
			opts:   cli.Options{Archive: archivePath, Chdir: root, Members: []string{"input"}, SplitSizeBytes: 1},
			ref:    localRef,
			want:   createStrategyFullPlan,
		},
		{
			name:   "stdout uses full plan",
			runner: transactional,
			opts:   cli.Options{Archive: "-", Chdir: root, Members: []string{"input"}},
			ref:    locator.Ref{Kind: locator.KindStdio, Raw: "-"},
			want:   createStrategyFullPlan,
		},
		{
			name:   "mixed input uses full plan",
			runner: transactional,
			opts:   cli.Options{Archive: archivePath, Chdir: root, Members: []string{"input", "s3://bucket/key"}},
			ref:    localRef,
			want:   createStrategyFullPlan,
		},
		{
			name:   "s3 output remains full plan",
			runner: transactional,
			opts:   cli.Options{Archive: "s3://bucket/archive.tar", Chdir: root, Members: []string{"input"}},
			ref:    locator.Ref{Kind: locator.KindS3, Raw: "s3://bucket/archive.tar", Bucket: "bucket", Key: "archive.tar"},
			want:   createStrategyFullPlan,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			request, err := tt.runner.prepareCreateRequest(context.Background(), tt.opts, nil)
			if err != nil {
				t.Fatalf("prepareCreateRequest() error = %v", err)
			}
			if got := tt.runner.selectCreateStrategy(request, tt.ref); got != tt.want {
				t.Fatalf("selectCreateStrategy() = %v, want %v", got, tt.want)
			}
		})
	}
}
