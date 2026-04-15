package engine

import (
	"fmt"
	"io"

	"github.com/islishude/gotgz/packages/archivepath"
	"github.com/islishude/gotgz/packages/locator"
)

// countingWriteCloser counts bytes written to the wrapped archive destination.
type countingWriteCloser struct {
	io.WriteCloser
	count int64
}

// Write forwards data and records how many bytes reached the destination.
func (w *countingWriteCloser) Write(p []byte) (int, error) {
	n, err := w.WriteCloser.Write(p)
	w.count += int64(n)
	return n, err
}

// archiveSplitRef builds the concrete path/key for one split output volume.
func archiveSplitRef(ref locator.Ref, part int, width int) locator.Ref {
	out := ref
	switch ref.Kind {
	case locator.KindLocal:
		out.Path = archivepath.FormatSplit(ref.Path, part, width)
		out.Raw = out.Path
	case locator.KindS3:
		out.Key = archivepath.FormatSplit(ref.Key, part, width)
		out.Raw = fmt.Sprintf("s3://%s/%s", out.Bucket, out.Key)
	}
	return out
}
