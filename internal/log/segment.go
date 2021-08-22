package log

import (
	"fmt"
	"google.golang.org/protobuf/proto"
	"os"
	"path"
	api "github.com/srikantrao/proglog/api/v1"
)

type segment struct {
	index      *index
	store      *store
	baseOffset uint64
	nextOffset uint64
	config     Config
}

// newSegment creates a new Segment starting at the baseOffset.
// It creates a new store and index and updates the nextOffset based on
// the last offset in the index.
func newSegment(dir string, baseOffset uint64, c Config) (*segment, error) {
	seg := &segment{
		baseOffset: baseOffset,
		config: c,
	}
	// Create the Store.
	storeFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%seg", baseOffset, ".store")),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644)
	if err != nil {
		return nil, err
	}
	seg.store, err = newStore(storeFile)
	if err != nil {
		return nil, err
	}

	// Create the Index
	indexFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%seg", baseOffset, ".index")),
		os.O_RDWR|os.O_CREATE,
		0644)
	if err != nil {
		return nil, err
	}
	seg.index, err = newIndex(indexFile, c)
	if err != nil {
		return nil, err
	}

	// Get the nextOffset
	off, _, err := seg.index.Read(-1)
	if err != nil {
		seg.nextOffset = baseOffset
	} else {
		seg.nextOffset = baseOffset + uint64(off) + 1 // next offset for index.
	}
	return seg, nil
}

// Append adds a new record to the segment
// Segment adds a new record at the nextOffset position by adding
// the relative offset to the index and appending the record to the store.
func (s *segment) Append(record *api.Record) (uint64, error) {
	currentOffset := s.nextOffset
	record.Offset = currentOffset
	msg, err := proto.Marshal(record)
	if err != nil {
		return 0, err
	}
	// Append the record to the store.
	_, position, err := s.store.Append(msg)
	if err != nil {
		return 0, err
	}
	// Write the index relative position to the index.
	if err = s.index.Write(uint32(s.nextOffset - s.baseOffset), position); err != nil {
		return 0, err
	}
	s.nextOffset++
	return currentOffset, nil
}

// Read returns the record for the given offset.
// offset is the absolute offset of the given record.
// Segment searches for index first and then uses the position in the store to
//retrieve the message.
func (s *segment) Read(offset uint64) (*api.Record, error) {
	// Get the relative offset
	// Get the position in the store where this record is stored.
	_, pos, err := s.index.Read(int64(offset - s.baseOffset))
	if err != nil {
		return nil, err
	}
	// Read the message from the store
	msg, err := s.store.Read(pos)
	if err != nil {
		return nil, err
	}
	record := &api.Record{}
	if err := proto.Unmarshal(msg, record); err != nil {
		return nil, err
	}
	return record, nil
}

// IsMaxed returns whether the segment has reached its maximum size.
func (s *segment) IsMaxed() bool{
	return s.store.size >= s.config.Segment.MaxStoreBytes ||
		s.index.size >= s.config.Segment.MaxIndexBytes
}

// Remove closes the segment and removes the store and index files.
func (s *segment) Remove() error {
	if err := s.Close(); err != nil {
		return err
	}
	if err := os.Remove(s.store.Name()); err != nil {
		return err
	}
	if err := os.Remove(s.index.Name()); err != nil {
		return err
	}
	return nil
}


func (s *segment) Close() error {
	if err := s.index.Close(); err != nil {
		return err
	}
	if err := s.store.Close(); err != nil {
		return err
	}
	return nil
}

// nearestMultiple returns the nearest and lesser multiple of k in j,
func nearestMultiple(j, k uint64) uint64 {
	if j >= 0 {
		return (j/k) * k
	}
	return ((j-k+1)/k) * k
}


