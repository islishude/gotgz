package engine

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/islishude/gotgz/packages/cli"
)

func TestRunUnsupportedModeFails(t *testing.T) {
	got := (&Runner{}).Run(context.Background(), cli.Options{Mode: cli.Mode("invalid")})
	if got.ExitCode != ExitFatal {
		t.Fatalf("ExitCode = %d, want %d", got.ExitCode, ExitFatal)
	}
	if got.Err == nil || !strings.Contains(got.Err.Error(), "unsupported mode") {
		t.Fatalf("Err = %v, want unsupported mode error", got.Err)
	}
}

func TestClassifyResult(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		warnings  int
		wantCode  int
		wantError bool
	}{
		{name: "success", wantCode: ExitSuccess},
		{name: "warning", warnings: 1, wantCode: ExitWarning},
		{name: "fatal", err: errors.New("boom"), wantCode: ExitFatal, wantError: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := classifyResult(tt.err, tt.warnings)
			if got.ExitCode != tt.wantCode {
				t.Fatalf("ExitCode = %d, want %d", got.ExitCode, tt.wantCode)
			}
			if (got.Err != nil) != tt.wantError {
				t.Fatalf("Err presence = %v, want %v", got.Err != nil, tt.wantError)
			}
		})
	}
}

func TestWarnfWithNilReporter(t *testing.T) {
	var stderr bytes.Buffer
	r := &Runner{stderr: &stderr}

	if warnings := r.warnf(nil, "test %s", "warning"); warnings != 1 {
		t.Fatalf("warnings = %d, want 1", warnings)
	}
	if got := stderr.String(); got != "gotgz: warning: test warning\n" {
		t.Fatalf("stderr = %q", got)
	}
}

func TestNormalizeCompressionHint(t *testing.T) {
	if got := normalizeCompressionHint(""); got != cli.CompressionAuto {
		t.Fatalf("normalizeCompressionHint(empty) = %q, want %q", got, cli.CompressionAuto)
	}
	if got := normalizeCompressionHint(cli.CompressionGzip); got != cli.CompressionGzip {
		t.Fatalf("normalizeCompressionHint(gzip) = %q, want %q", got, cli.CompressionGzip)
	}
}
