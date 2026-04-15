package compress

import "testing"

// TestFromString verifies that supported names map to concrete compression types
// and unknown values fall back to auto detection.
func TestFromString(t *testing.T) {
	tests := []struct {
		input string
		want  Type
	}{
		{input: "none", want: None},
		{input: "GZIP", want: Gzip},
		{input: "bzip2", want: Bzip2},
		{input: "xz", want: Xz},
		{input: "zstd", want: Zstd},
		{input: "lz4", want: Lz4},
		{input: "unknown", want: Auto},
	}

	for _, tt := range tests {
		if got := FromString(tt.input); got != tt.want {
			t.Fatalf("FromString(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}
