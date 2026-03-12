package engine

import "testing"

func TestShouldRotateSplitZipVolume(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		written   int64
		splitSize int64
		want      bool
	}{
		{
			name:      "disabled split never rotates",
			written:   128,
			splitSize: 0,
			want:      false,
		},
		{
			name:      "tiny split always rotates after each entry",
			written:   0,
			splitSize: splitZipEntryFinalizeLookaheadBytes,
			want:      true,
		},
		{
			name:      "far from threshold does not rotate",
			written:   128,
			splitSize: 1024,
			want:      false,
		},
		{
			name:      "lookahead margin rotates before threshold",
			written:   1024 - splitZipEntryFinalizeLookaheadBytes,
			splitSize: 1024,
			want:      true,
		},
		{
			name:      "past threshold rotates",
			written:   2048,
			splitSize: 1024,
			want:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := shouldRotateSplitZipVolume(tt.written, tt.splitSize); got != tt.want {
				t.Fatalf("shouldRotateSplitZipVolume(%d, %d) = %v, want %v", tt.written, tt.splitSize, got, tt.want)
			}
		})
	}
}
