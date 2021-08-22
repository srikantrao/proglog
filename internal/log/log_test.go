package log

import (
	api "github.com/srikantrao/proglog/api/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"io/ioutil"
	"os"
	"testing"
)

func TestLog(t *testing.T) {
	for scenario, fn := range map[string]func(t *testing.T, l *Log){
		"append and read a record succeeds": testAppendRead,
		"offset out of bounds error":        testOutOfRangeErr,
		"init with existing segments":       testInitSegments,
		"testing the truncate code":         testTruncate,
		"testing the reader code":           testReader,
	} {
		t.Run(scenario, func(t *testing.T) {
			dir, err := ioutil.TempDir("", "store-test")
			require.NoError(t, err)
			defer os.RemoveAll(dir)
			c := Config{}
			c.Segment.MaxStoreBytes = 32
			log, err := NewLog(dir, c)
			require.NoError(t, err)
			fn(t, log)
		})
	}
}

// TestAppendRead tests that we can successfully append to and  read from the log
func testAppendRead(t *testing.T, l *Log) {
	testMessage := &api.Record{
		Value: []byte("Hello World!"),
	}
	offset, err := l.Append(testMessage)
	require.NoError(t, err)
	require.Equal(t, uint64(0), offset)

	// Retrieve the message
	readMessage, err := l.Read(offset)
	require.NoError(t, err)
	require.Equal(t, testMessage.Value, readMessage.Value)

}


func testOutOfRangeErr(t *testing.T, log *Log) {
	read, err := log.Read(1)
	require.Nil(t, read)
	apiErr := err.(api.ErrOffsetOutOfRange)
	require.Equal(t, uint64(1), apiErr.Offset)
}

func testInitSegments(t *testing.T, l *Log) {
	// Create a record
	testRecord := &api.Record{
		Value: []byte("Hello World!"),
	}

	// Append the same contents 3 times
	for i := 0; i < 3; i++ {
		currentOffset, err := l.Append(testRecord)
		require.NoError(t, err)
		require.Equal(t, uint64(i), currentOffset)
	}

	// Get the Lowest and Highest Offset
	offset, err := l.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(0), offset)

	offset, err = l.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(2), offset)

	// Close the Log now
	require.NoError(t, l.Close())

	// Open up the Log once again
	newLog, err := NewLog(l.Dir, l.Config)
	require.NoError(t, err)

	// Make sure the lowest and highest offset are still the same
	lowestOffset, err := newLog.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(0), lowestOffset)

	highestOffset, err := newLog.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(2), highestOffset)

	require.NoError(t, newLog.Close())
}

func testTruncate(t *testing.T, l *Log) {
	// Create a record
	testRecord := &api.Record{
		Value: []byte("Hello World!"),
	}

	// Append the same contents 3 times
	for i := 0; i < 2; i++ {
		currentOffset, err := l.Append(testRecord)
		require.NoError(t, err)
		require.Equal(t, uint64(i), currentOffset)
	}

	// Truncate
	err := l.Truncate(3)
	require.NoError(t, err)

	_, err = l.Read(0)
	require.Error(t, err)
}

func testReader(t *testing.T, log *Log) {
	testRecord := &api.Record{
		Value: []byte("hello world"),
	}
	off, err := log.Append(testRecord)
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	reader := log.Reader()
	b, err := ioutil.ReadAll(reader)
	require.NoError(t, err)

	read := &api.Record{}
	err = proto.Unmarshal(b[lenWidth:], read)
	require.NoError(t, err)
	require.Equal(t, testRecord.Value, read.Value)
}
