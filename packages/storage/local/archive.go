package local

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/islishude/gotgz/packages/archive"
	"github.com/islishude/gotgz/packages/locator"
)

type ArchiveStore struct{}

type Metadata struct {
	Size int64
}

// WriteCapabilities describes publication and artifact guarantees available
// before a destination writer is opened.
type WriteCapabilities struct {
	RollbackSafe        bool
	SingleLogicalOutput bool
	ExposesTempPaths    bool
}

// WriteCapabilities reports local destination guarantees without opening or
// modifying the target.
func (s *ArchiveStore) WriteCapabilities(ref locator.Ref) WriteCapabilities {
	_ = s
	switch ref.Kind {
	case locator.KindLocal:
		return WriteCapabilities{
			RollbackSafe:        true,
			SingleLogicalOutput: true,
			ExposesTempPaths:    true,
		}
	case locator.KindStdio:
		return WriteCapabilities{SingleLogicalOutput: true}
	default:
		return WriteCapabilities{}
	}
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
	session, err := s.BeginWriter(ref)
	if err != nil {
		return nil, err
	}
	return session, nil
}

// WriteSession is an archive destination that becomes visible only after
// Commit. Close is retained as a compatibility alias for Commit.
type WriteSession interface {
	io.WriteCloser
	Commit() error
	Abort(error) error
}

// BeginWriter opens a transactional local or stdio archive destination.
func (s *ArchiveStore) BeginWriter(ref locator.Ref) (WriteSession, error) {
	switch ref.Kind {
	case locator.KindLocal:
		return beginLocalFileWrite(ref.Path)
	case locator.KindStdio:
		return &stdioWriteSession{w: os.Stdout}, nil
	default:
		return nil, fmt.Errorf("unsupported local archive ref kind %s", ref.Kind)
	}
}

type localFileWriteSession struct {
	mu             sync.Mutex
	file           *os.File
	tempPath       string
	target         string
	targetMetadata *existingTargetMetadata
	finished       bool
	committed      bool
	err            error
}

func beginLocalFileWrite(target string) (*localFileWriteSession, error) {
	target = filepath.Clean(target)
	mode := os.FileMode(0o666) &^ archive.CurrentUmask()
	var targetMetadata *existingTargetMetadata
	if info, err := os.Lstat(target); err == nil {
		if !info.Mode().IsRegular() {
			return nil, fmt.Errorf("archive target %q must be a regular file", target)
		}
		targetMetadata, err = captureExistingTargetMetadata(target, info)
		if err != nil {
			return nil, err
		}
		mode = targetMetadata.mode.Perm()
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	dir := filepath.Dir(target)
	temp, err := os.CreateTemp(dir, "."+filepath.Base(target)+".gotgz-*")
	if err != nil {
		return nil, err
	}
	if err := temp.Chmod(mode); err != nil {
		_ = temp.Close()
		_ = os.Remove(temp.Name())
		return nil, err
	}
	return &localFileWriteSession{
		file:           temp,
		tempPath:       temp.Name(),
		target:         target,
		targetMetadata: targetMetadata,
	}, nil
}

func (w *localFileWriteSession) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.finished {
		return 0, os.ErrClosed
	}
	return w.file.Write(p)
}

// EphemeralLocalPaths returns the exact transaction artifacts that must be
// excluded if archive planning overlaps destination writes.
func (w *localFileWriteSession) EphemeralLocalPaths() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.tempPath == "" {
		return nil
	}
	return []string{w.tempPath}
}

func (w *localFileWriteSession) Commit() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.finished {
		return w.err
	}
	w.finished = true
	if err := w.targetMetadata.revalidate(w.target); err != nil {
		w.err = err
	}
	if w.err == nil {
		w.err = w.targetMetadata.apply(w.file, w.tempPath)
	}
	if w.err == nil {
		w.err = w.file.Sync()
	}
	if err := w.file.Close(); err != nil {
		w.err = errors.Join(w.err, err)
	}
	if w.err == nil {
		w.err = w.targetMetadata.revalidate(w.target)
	}
	if w.err == nil {
		w.err = os.Rename(w.tempPath, w.target)
	}
	if w.err != nil {
		_ = os.Remove(w.tempPath)
		return w.err
	}
	w.committed = true
	return nil
}

func (w *localFileWriteSession) Abort(_ error) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.committed {
		return nil
	}
	if !w.finished {
		w.finished = true
		w.err = w.file.Close()
	}
	removeErr := os.Remove(w.tempPath)
	if errors.Is(removeErr, os.ErrNotExist) {
		removeErr = nil
	}
	return errors.Join(w.err, removeErr)
}

func (w *localFileWriteSession) Close() error { return w.Commit() }

type stdioWriteSession struct{ w io.Writer }

func (w *stdioWriteSession) Write(p []byte) (int, error) { return w.w.Write(p) }
func (*stdioWriteSession) Commit() error                 { return nil }
func (*stdioWriteSession) Abort(error) error             { return nil }
func (w *stdioWriteSession) Close() error                { return w.Commit() }
