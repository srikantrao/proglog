package log

import (
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"os"
	"testing"
)

var (
	write = []byte("Hello World!")
	width = uint64(len(write) + lenWidth)
)

func TestStoreAppendRead(t *testing.T) {
	file, err := ioutil.TempFile("", "store_append_read_test")
	require.NoError(t, err)
	defer os.Remove(file.Name())

	// Create a Store
	s, err := newStore(file)
	require.NoError(t, err)

	testAppend(t, s)
	testRead(t, s)
	testReadAt(t, s)

	// Recover state test
	s, err = newStore(file)
	require.NoError(t, err)
	testRead(t, s)
}

func testAppend(t *testing.T, s *store) {
	t.Helper()
	for i := 1; i < 4; i++ {
		n, pos, err := s.Append(write)
		require.NoError(t, err)
		require.Equal(t, pos+n, width*uint64(i))
	}
}

func testRead(t *testing.T, s *store) {
	t.Helper()
	var pos uint64
	for i := 1; i < 4; i++ {
		record, err := s.Read(pos)
		require.NoError(t, err)
		require.Equal(t, record, write)
		pos += width
	}
}

func testReadAt(t *testing.T, s *store) {
	t.Helper()
	var offset int64
	for i := 1; i < 4; i++ {
		// Read the size of the message
		messageLength := make([]byte, lenWidth)
		numBytesRead, err := s.ReadAt(messageLength, offset)
		require.NoError(t, err)
		require.Equal(t, lenWidth, numBytesRead)
		offset += int64(numBytesRead)

		// Read the message itself
		messageSize := enc.Uint64(messageLength)
		messageBytes := make([]byte, messageSize)
		numBytesRead, err = s.ReadAt(messageBytes, offset)
		require.NoError(t, err)
		require.Equal(t, write, messageBytes)
		require.Equal(t, int(messageSize), numBytesRead)
		offset += int64(numBytesRead)
	}
}

func TestStoreClose(t *testing.T) {
	f, err := ioutil.TempFile("", "store_close_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	s, err := newStore(f)
	require.NoError(t, err)
	_, _, err = s.Append(write)
	require.NoError(t, err)

	f, beforeSize, err := openFile(f.Name())
	require.NoError(t, err)
	err = s.Close()
	require.NoError(t, err)
	_, afterSize, err := openFile(f.Name())
	require.NoError(t, err)
	require.True(t, afterSize > beforeSize)

}

func openFile(name string) (file *os.File, size int64, err error) {
	f, err := os.OpenFile(
		name,
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, 0, err
	}
	fi, err := f.Stat()
	if err != nil {
		return nil, 0, err
	}
	return f, fi.Size(), nil
}
