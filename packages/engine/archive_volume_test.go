package engine

import (
	"context"
	"math"
	"strings"
	"testing"

	"github.com/islishude/gotgz/packages/locator"
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

func TestResolveArchiveVolumes(t *testing.T) {
	t.Parallel()

	t.Run("split part not 1 returns error", func(t *testing.T) {
		t.Parallel()
		r := &Runner{}
		ref := locator.Ref{
			Kind: locator.KindLocal,
			Raw:  "archive.part0003.tar.gz",
			Path: "archive.part0003.tar.gz",
		}
		_, err := r.resolveArchiveVolumes(context.Background(), ref, archiveReaderInfo{})
		if err == nil || !strings.Contains(err.Error(), "split archives must be opened with part0001") {
			t.Fatalf("resolveArchiveVolumes() error = %v, want part0001 error", err)
		}
	})

	t.Run("http split archive returns error", func(t *testing.T) {
		t.Parallel()
		r := &Runner{}
		ref := locator.Ref{
			Kind: locator.KindHTTP,
			Raw:  "https://example.com/archive.part0001.tar.gz",
			URL:  "https://example.com/archive.part0001.tar.gz",
		}
		_, err := r.resolveArchiveVolumes(context.Background(), ref, archiveReaderInfo{})
		if err == nil || !strings.Contains(err.Error(), "http(s) split archives are not supported") {
			t.Fatalf("resolveArchiveVolumes() error = %v, want http(s) split error", err)
		}
	})

	t.Run("unknown kind with split name returns single volume", func(t *testing.T) {
		t.Parallel()
		r := &Runner{}
		ref := locator.Ref{
			Kind: locator.KindStdio,
			Raw:  "archive.part0001.tar.gz",
		}
		info := archiveReaderInfo{Size: 100, SizeKnown: true}
		vols, err := r.resolveArchiveVolumes(context.Background(), ref, info)
		if err != nil {
			t.Fatalf("resolveArchiveVolumes() unexpected error: %v", err)
		}
		if len(vols) != 1 {
			t.Fatalf("resolveArchiveVolumes() got %d volumes, want 1", len(vols))
		}
		if vols[0].ref.Kind != ref.Kind || vols[0].ref.Raw != ref.Raw || vols[0].info != info {
			t.Fatalf("resolveArchiveVolumes() returned unexpected volume")
		}
	})

	t.Run("non-split name returns single volume", func(t *testing.T) {
		t.Parallel()
		r := &Runner{}
		ref := locator.Ref{
			Kind: locator.KindHTTP,
			Raw:  "https://example.com/archive.tar.gz",
			URL:  "https://example.com/archive.tar.gz",
		}
		info := archiveReaderInfo{Size: 50, SizeKnown: true}
		vols, err := r.resolveArchiveVolumes(context.Background(), ref, info)
		if err != nil {
			t.Fatalf("resolveArchiveVolumes() unexpected error: %v", err)
		}
		if len(vols) != 1 {
			t.Fatalf("resolveArchiveVolumes() got %d volumes, want 1", len(vols))
		}
		if vols[0].ref.Kind != ref.Kind || vols[0].ref.Raw != ref.Raw || vols[0].info != info {
			t.Fatalf("resolveArchiveVolumes() returned unexpected volume")
		}
	})
}
