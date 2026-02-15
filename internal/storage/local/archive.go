package local

import (
	"fmt"
	"io"
	"os"

	"github.com/islishude/gotgz/internal/locator"
)

type ArchiveStore struct{}

type Metadata struct {
	Size int64
}

func (s *ArchiveStore) OpenReader(ref locator.Ref) (io.ReadCloser, Metadata, error) {
	switch ref.Kind {
	case locator.KindLocal:
		f, err := os.Open(ref.Path)
		if err != nil {
			return nil, Metadata{}, err
		}
		st, _ := f.Stat()
		meta := Metadata{}
		if st != nil {
			meta.Size = st.Size()
		}
		return f, meta, nil
	case locator.KindStdio:
		return io.NopCloser(os.Stdin), Metadata{}, nil
	default:
		return nil, Metadata{}, fmt.Errorf("unsupported local archive ref kind %s", ref.Kind)
	}
}

func (s *ArchiveStore) OpenWriter(ref locator.Ref) (io.WriteCloser, error) {
	switch ref.Kind {
	case locator.KindLocal:
		return os.Create(ref.Path)
	case locator.KindStdio:
		return nopWriteCloser{w: os.Stdout}, nil
	default:
		return nil, fmt.Errorf("unsupported local archive ref kind %s", ref.Kind)
	}
}

type nopWriteCloser struct{ w io.Writer }

func (n nopWriteCloser) Write(p []byte) (int, error) { return n.w.Write(p) }
func (nopWriteCloser) Close() error                  { return nil }
