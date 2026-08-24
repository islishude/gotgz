package engine

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
)

const maxStreamingPlanRecordBytes = 16 << 20

const (
	streamingPlanPublishBytes   = 64 << 10
	streamingPlanPublishRecords = 32
	streamingPlanWarmupRecords  = 4
)

type planTailReader interface {
	Next(ctx context.Context) (createPlanRecord, error)
}

// streamingMemberSpool is a private disk-backed ordered queue. Append only
// publishes a frame after its complete bytes have reached the spool file.
type streamingMemberSpool struct {
	mu sync.Mutex

	file            *os.File
	writer          *bufio.Writer
	path            string
	published       int64
	unpublished     int64
	pendingRecords  int
	recordsAppended int64
	readOffset      int64
	notify          chan struct{}

	producerDone  bool
	producerErr   error
	producerTotal int64

	closeOnce sync.Once
	closeErr  error
}

func newStreamingMemberSpool(dir string) (*streamingMemberSpool, error) {
	file, err := os.CreateTemp(dir, "stream-member-*.plan")
	if err != nil {
		return nil, fmt.Errorf("create streaming plan spool: %w", err)
	}
	return &streamingMemberSpool{
		file:   file,
		writer: bufio.NewWriterSize(file, streamingPlanPublishBytes),
		path:   file.Name(),
		notify: make(chan struct{}),
	}, nil
}

func (s *streamingMemberSpool) Append(record createPlanRecord) error {
	currentLength := len(record.Current)
	archiveNameLength := len(record.ArchiveName)
	payloadLength := currentLength + archiveNameLength
	if payloadLength <= 0 || payloadLength > maxStreamingPlanRecordBytes {
		return fmt.Errorf("streaming plan record size %d is outside the supported range", payloadLength)
	}
	frame := make([]byte, 8+payloadLength)
	binary.BigEndian.PutUint32(frame[:4], uint32(currentLength))
	binary.BigEndian.PutUint32(frame[4:8], uint32(archiveNameLength))
	copy(frame[8:], record.Current)
	copy(frame[8+currentLength:], record.ArchiveName)

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.producerDone {
		return fmt.Errorf("append streaming plan record after producer completion")
	}
	n, err := s.writer.Write(frame)
	if err == nil && n != len(frame) {
		err = io.ErrShortWrite
	}
	if err != nil {
		return fmt.Errorf("append streaming plan spool %q: %w", s.path, err)
	}
	s.unpublished += int64(len(frame))
	s.pendingRecords++
	s.recordsAppended++
	if s.recordsAppended <= streamingPlanWarmupRecords || s.unpublished >= streamingPlanPublishBytes || s.pendingRecords >= streamingPlanPublishRecords {
		if err := s.publishLocked(); err != nil {
			return err
		}
	}
	return nil
}

func (s *streamingMemberSpool) Finish(total int64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.producerDone {
		return
	}
	if publishErr := s.publishLocked(); publishErr != nil {
		err = errors.Join(err, publishErr)
	}
	s.producerDone = true
	s.producerErr = err
	s.producerTotal = total
	s.signalLocked()
}

func (s *streamingMemberSpool) Next(ctx context.Context) (createPlanRecord, error) {
	for {
		s.mu.Lock()
		if s.readOffset < s.published {
			offset := s.readOffset
			published := s.published
			s.mu.Unlock()

			record, next, err := readStreamingPlanFrame(s.file, s.path, offset, published)
			if err != nil {
				return createPlanRecord{}, err
			}
			s.mu.Lock()
			if s.readOffset != offset {
				s.mu.Unlock()
				return createPlanRecord{}, fmt.Errorf("streaming plan spool %q has multiple concurrent readers", s.path)
			}
			s.readOffset = next
			s.mu.Unlock()
			return record, nil
		}
		if s.producerDone {
			err := s.producerErr
			s.mu.Unlock()
			if err != nil {
				return createPlanRecord{}, err
			}
			return createPlanRecord{}, io.EOF
		}
		notify := s.notify
		s.mu.Unlock()

		select {
		case <-notify:
		case <-ctx.Done():
			return createPlanRecord{}, ctx.Err()
		}
	}
}

func readStreamingPlanFrame(file *os.File, path string, offset, published int64) (createPlanRecord, int64, error) {
	if published-offset < 8 {
		return createPlanRecord{}, offset, fmt.Errorf("streaming plan spool %q published a partial frame header", path)
	}
	var header [8]byte
	if _, err := file.ReadAt(header[:], offset); err != nil {
		return createPlanRecord{}, offset, fmt.Errorf("read streaming plan frame header %q: %w", path, err)
	}
	currentLength := uint64(binary.BigEndian.Uint32(header[:4]))
	archiveNameLength := uint64(binary.BigEndian.Uint32(header[4:8]))
	length := currentLength + archiveNameLength
	if length == 0 || length > maxStreamingPlanRecordBytes {
		return createPlanRecord{}, offset, fmt.Errorf("streaming plan spool %q contains invalid frame length %d", path, length)
	}
	if length > uint64(published-offset-8) {
		return createPlanRecord{}, offset, fmt.Errorf("streaming plan spool %q published a partial frame", path)
	}
	payload := make([]byte, int(length))
	if _, err := file.ReadAt(payload, offset+8); err != nil {
		return createPlanRecord{}, offset, fmt.Errorf("read streaming plan frame %q: %w", path, err)
	}
	record := createPlanRecord{
		Current:     string(payload[:currentLength]),
		ArchiveName: string(payload[currentLength:]),
	}
	return record, offset + 8 + int64(length), nil
}

func (s *streamingMemberSpool) signalLocked() {
	close(s.notify)
	s.notify = make(chan struct{})
}

func (s *streamingMemberSpool) publishLocked() error {
	if s.unpublished == 0 {
		return nil
	}
	if err := s.writer.Flush(); err != nil {
		return fmt.Errorf("publish streaming plan spool %q: %w", s.path, err)
	}
	s.published += s.unpublished
	s.unpublished = 0
	s.pendingRecords = 0
	s.signalLocked()
	return nil
}

func (s *streamingMemberSpool) Close() error {
	if s == nil {
		return nil
	}
	s.closeOnce.Do(func() {
		closeErr := s.file.Close()
		removeErr := os.Remove(s.path)
		if errors.Is(removeErr, os.ErrNotExist) {
			removeErr = nil
		}
		if closeErr != nil {
			closeErr = fmt.Errorf("close streaming plan spool %q: %w", s.path, closeErr)
		}
		if removeErr != nil {
			removeErr = fmt.Errorf("remove streaming plan spool %q: %w", s.path, removeErr)
		}
		s.closeErr = errors.Join(closeErr, removeErr)
	})
	return s.closeErr
}
