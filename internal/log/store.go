package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var (
	enc = binary.BigEndian
)

const (
	lenWidth = 8
)

// wrapper around a file.
type store struct {
	*os.File
	mu   sync.Mutex
	buf  *bufio.Writer
	size uint64
}

func newStore(f *os.File) (*store, error) {
	fileInfo, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	size := uint64(fileInfo.Size())
	return &store{
		File: f,
		size: size,
		buf:  bufio.NewWriter(f),
	}, nil
}

// Append persists the record to the store
func (s *store) Append(p []byte) (n, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// starting position of the message
	pos = s.size
	// write the length of the message to the buffer
	if err := binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		return 0, 0, err
	}
	// write the message to the buffer
	messageLength, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}
	messageLength += lenWidth
	s.size += uint64(messageLength)
	return uint64(messageLength), pos, nil
}

// Read returns the record stored at the given position
func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Flush th write buffer before attempting to read.
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}
	// get the length of the message
	messageLength := make([]byte, lenWidth)
	if _, err := s.File.ReadAt(messageLength, int64(pos)); err != nil {
		return nil, err
	}
	// read the message
	message := make([]byte, enc.Uint64(messageLength))
	if _, err := s.File.ReadAt(message, int64(pos+lenWidth)); err  != nil {
		return nil, err
	}
	return message, nil
}

// ReadAt reads len(p) bytes into p beginning at the off offset in the
// store's file.
func (s *store) ReadAt(p []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Empty the write buffer
	if err := s.buf.Flush(); err != nil {
		return 0, err
	}
	return s.File.ReadAt(p, off)
}

// persists any buffered data before closing the store
func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
		return err
	}
	return s.File.Close()
}
