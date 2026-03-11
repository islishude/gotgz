package engine

import "testing"

func TestShouldFlushSplitVolume(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		written   int64
		splitSize int64
		want      bool
	}{
		{
			name:      "small split stays exact",
			written:   0,
			splitSize: 512 * 1024,
			want:      true,
		},
		{
			name:      "far from threshold skips flush",
			written:   4 << 20,
			splitSize: 10 << 20,
			want:      false,
		},
		{
			name:      "near threshold flushes",
			written:   9 << 20,
			splitSize: 10 << 20,
			want:      true,
		},
		{
			name:      "past threshold flushes",
			written:   11 << 20,
			splitSize: 10 << 20,
			want:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := shouldFlushSplitVolume(tt.written, tt.splitSize); got != tt.want {
				t.Fatalf("shouldFlushSplitVolume(%d, %d) = %v, want %v", tt.written, tt.splitSize, got, tt.want)
			}
		})
	}
}
