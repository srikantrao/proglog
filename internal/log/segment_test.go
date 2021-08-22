package log

import (
	api "github.com/srikantrao/proglog/api/v1"
	"github.com/stretchr/testify/require"
	"io"
	"io/ioutil"
	"os"
	"testing"
)

func TestSegment(t *testing.T) {
	dir, _ := ioutil.TempDir("", "segment-test")
	defer os.RemoveAll(dir)

	testRecord := &api.Record{
		Value:  []byte("Hello world!"),
	}
	c := Config{}
	c.Segment.MaxIndexBytes = entWidth * 3
	c.Segment.MaxStoreBytes = 1024

	s, err := newSegment(dir, 16, c)
	require.NoError(t, err)
	require.Equal(t, uint64(16), s.nextOffset)
	require.False(t, s.IsMaxed())

	// Add all 3 records to max the index size out.
	for i := 0; i < 3; i++ {
		off, err := s.Append(testRecord)
		require.NoError(t, err)
		require.Equal(t, uint64(16+i), off)

		got, err := s.Read(off)
		require.NoError(t, err)
		require.Equal(t, testRecord.Value, got.Value)
	}

	_, err = s.Append(testRecord)
	require.Equal(t, io.EOF, err)

	// Index should be maxed out
	require.True(t, s.IsMaxed())

	// Change Segment max sizes
	c.Segment.MaxStoreBytes = uint64(len(testRecord.Value) * 3)
	c.Segment.MaxIndexBytes = 1024

	s, err = newSegment(dir, 16, c)
	require.NoError(t, err)

	// This should still be maxed out since Segment store is already full.
	require.True(t, s.IsMaxed())

	err = s.Remove()
	require.NoError(t, err)
	s, err = newSegment(dir, 16, c)
	require.NoError(t, err)
	require.False(t, s.IsMaxed())
}
