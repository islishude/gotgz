package engine

import (
	"math"
	"testing"
)

func TestSumArchiveVolumeSizes(t *testing.T) {
	t.Parallel()

	t.Run("unknown size reports false", func(t *testing.T) {
		t.Parallel()

		total, known := sumArchiveVolumeSizes([]archiveVolume{
			{info: archiveReaderInfo{Size: 10, SizeKnown: true}},
			{info: archiveReaderInfo{SizeKnown: false}},
		})
		if total != 0 || known {
			t.Fatalf("sumArchiveVolumeSizes() = (%d, %v), want (0, false)", total, known)
		}
	})

	t.Run("overflow clamps to max int64", func(t *testing.T) {
		t.Parallel()

		total, known := sumArchiveVolumeSizes([]archiveVolume{
			{info: archiveReaderInfo{Size: math.MaxInt64 - 10, SizeKnown: true}},
			{info: archiveReaderInfo{Size: 42, SizeKnown: true}},
		})
		if total != math.MaxInt64 || !known {
			t.Fatalf("sumArchiveVolumeSizes() = (%d, %v), want (%d, true)", total, known, int64(math.MaxInt64))
		}
	})
}
