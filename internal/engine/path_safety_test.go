package engine

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestSafeSymlinkTarget(t *testing.T) {
	base := filepath.Join(t.TempDir(), "extract")
	symlinkPath := filepath.Join(base, "sub", "link")

	tests := []struct {
		name     string
		linkname string
		wantErr  string // non-empty substring expected in error
	}{
		{name: "empty target", linkname: "", wantErr: "symlink target is empty"},
		{name: "relative within base", linkname: "../file.txt", wantErr: ""},
		{name: "relative escapes base", linkname: "../../../etc/passwd", wantErr: "escapes extraction directory"},
		{name: "absolute target", linkname: "/etc/passwd", wantErr: "absolute symlink target not allowed"},
		{name: "absolute target within tree", linkname: "/sub/other", wantErr: "absolute symlink target not allowed"},
		{name: "relative self-referencing", linkname: ".", wantErr: ""},
		{name: "relative deeper", linkname: "deeper/file", wantErr: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := safeSymlinkTarget(base, symlinkPath, tt.linkname)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("expected error containing %q, got: %v", tt.wantErr, err)
			}
		})
	}
}
