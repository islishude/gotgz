package archiveutil

import (
	"reflect"
	"testing"
)

// TestMergeMetadata verifies merge precedence and empty-input behavior.
func TestMergeMetadata(t *testing.T) {
	tests := []struct {
		name    string
		base    map[string]string
		overlay map[string]string
		want    map[string]string
	}{
		{
			name:    "nil maps return nil",
			base:    nil,
			overlay: nil,
			want:    nil,
		},
		{
			name:    "empty maps return nil",
			base:    map[string]string{},
			overlay: map[string]string{},
			want:    nil,
		},
		{
			name:    "overlay wins on key conflict",
			base:    map[string]string{"owner": "platform", "team": "storage"},
			overlay: map[string]string{"team": "archive", "trace": "enabled"},
			want:    map[string]string{"owner": "platform", "team": "archive", "trace": "enabled"},
		},
		{
			name:    "base only",
			base:    map[string]string{"a": "1"},
			overlay: nil,
			want:    map[string]string{"a": "1"},
		},
		{
			name:    "overlay only",
			base:    nil,
			overlay: map[string]string{"b": "2"},
			want:    map[string]string{"b": "2"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := MergeMetadata(tc.base, tc.overlay)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("MergeMetadata() = %#v, want %#v", got, tc.want)
			}
		})
	}
}

// TestMergeMetadata_DoesNotMutateInputs verifies that merge output is decoupled
// from both input maps.
func TestMergeMetadata_DoesNotMutateInputs(t *testing.T) {
	base := map[string]string{"owner": "platform", "team": "storage"}
	overlay := map[string]string{"team": "archive", "trace": "enabled"}

	got := MergeMetadata(base, overlay)
	if got == nil {
		t.Fatal("MergeMetadata() = nil, want non-nil map")
	}

	got["owner"] = "changed"
	got["trace"] = "updated"

	if !reflect.DeepEqual(base, map[string]string{"owner": "platform", "team": "storage"}) {
		t.Fatalf("base map mutated: %#v", base)
	}
	if !reflect.DeepEqual(overlay, map[string]string{"team": "archive", "trace": "enabled"}) {
		t.Fatalf("overlay map mutated: %#v", overlay)
	}
}
