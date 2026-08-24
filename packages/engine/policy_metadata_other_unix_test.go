//go:build unix && !linux

package engine

import (
	"bytes"
	"testing"
	"time"

	"github.com/islishude/gotgz/packages/archiveprogress"
	"github.com/islishude/gotgz/packages/cli"
)

func TestEffectiveMetadataPolicyWarnsForUnsupportedACL(t *testing.T) {
	var stderr bytes.Buffer
	runner := &Runner{stderr: &stderr}
	reporter := archiveprogress.NewReporter(&stderr, cli.ProgressNever, 0, false, time.Now(), false)
	policy, warnings := runner.effectiveMetadataPolicy(cli.Options{ACL: true}, reporter)
	if policy.ACL || warnings != 1 {
		t.Fatalf("policy=%+v warnings=%d, want ACL disabled and one warning", policy, warnings)
	}
	if !bytes.Contains(stderr.Bytes(), []byte("only supported on Linux")) {
		t.Fatalf("stderr = %q", stderr.String())
	}
}
