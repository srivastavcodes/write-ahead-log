package wal

import (
	"io"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func destroyWAL(wal *Wal) {
	if wal != nil {
		_ = wal.Close()
		_ = os.RemoveAll(wal.options.DirPath)
	}
}

func TestWAL_WriteALL(t *testing.T) {
	dir, _ := os.MkdirTemp("", "wal-test-write-batch-1")
	opts := Options{
		DirPath:        dir,
		SegmentFileExt: ".SEG",
		SegmentSize:    32 * 1024 * 1024,
	}
	wal, err := Open(opts)
	require.Nil(t, err)
	defer destroyWAL(wal)

	testWriteAllIterate(t, wal, 0, 10)
	require.True(t, wal.IsEmpty())

	testWriteAllIterate(t, wal, 10000, 512)
	require.False(t, wal.IsEmpty())
}

func TestWAL_Write(t *testing.T) {
	dir, _ := os.MkdirTemp("", "wal-test-write1")
	opts := Options{
		DirPath:        dir,
		SegmentFileExt: ".SEG",
		SegmentSize:    32 * 1024 * 1024,
	}
	wal, err := Open(opts)
	require.Nil(t, err)
	defer destroyWAL(wal)

	// write 1
	pos1, err := wal.Write([]byte("hello1"))
	require.Nil(t, err)
	require.NotNil(t, pos1)
	pos2, err := wal.Write([]byte("hello2"))
	require.Nil(t, err)
	require.NotNil(t, pos2)
	pos3, err := wal.Write([]byte("hello3"))
	require.Nil(t, err)
	require.NotNil(t, pos3)

	val, err := wal.Read(pos1)
	require.Nil(t, err)
	require.Equal(t, "hello1", string(val))
	val, err = wal.Read(pos2)
	require.Nil(t, err)
	require.Equal(t, "hello2", string(val))
	val, err = wal.Read(pos3)
	require.Nil(t, err)
	require.Equal(t, "hello3", string(val))
}

func TestWAL_Write_large(t *testing.T) {
	dir, _ := os.MkdirTemp("", "wal-test-write2")
	opts := Options{
		DirPath:        dir,
		SegmentFileExt: ".SEG",
		SegmentSize:    32 * 1024 * 1024,
	}
	wal, err := Open(opts)
	require.Nil(t, err)
	defer destroyWAL(wal)

	testWriteAndIterate(t, wal, 100000, 512)
}

func TestWAL_Write_large2(t *testing.T) {
	dir, _ := os.MkdirTemp("", "wal-test-write3")
	opts := Options{
		DirPath:        dir,
		SegmentFileExt: ".SEG",
		SegmentSize:    32 * 1024 * 1024,
	}
	wal, err := Open(opts)
	require.Nil(t, err)
	defer destroyWAL(wal)

	testWriteAndIterate(t, wal, 2000, 32*1024*3+10)
}

func TestWAL_OpenNewActiveSegment(t *testing.T) {
	dir, _ := os.MkdirTemp("", "wal-test-new-active-segment")
	opts := Options{
		DirPath:        dir,
		SegmentFileExt: ".SEG",
		SegmentSize:    32 * 1024 * 1024,
	}
	wal, err := Open(opts)
	require.Nil(t, err)
	defer destroyWAL(wal)

	testWriteAndIterate(t, wal, 2000, 512)
	err = wal.OpenNewActiveSegment()
	require.Nil(t, err)

	val := strings.Repeat("wal", 100)
	for i := 0; i < 100; i++ {
		pos, err := wal.Write([]byte(val))
		require.Nil(t, err)
		require.NotNil(t, pos)
	}
}

func TestWAL_IsEmpty(t *testing.T) {
	dir, _ := os.MkdirTemp("", "wal-test-is-empty")
	opts := Options{
		DirPath:        dir,
		SegmentFileExt: ".SEG",
		SegmentSize:    32 * 1024 * 1024,
	}
	wal, err := Open(opts)
	require.Nil(t, err)
	defer destroyWAL(wal)

	require.True(t, wal.IsEmpty())
	testWriteAndIterate(t, wal, 2000, 512)
	require.False(t, wal.IsEmpty())
}

func TestWAL_Reader(t *testing.T) {
	dir, _ := os.MkdirTemp("", "wal-test-wal-reader")
	opts := Options{
		DirPath:        dir,
		SegmentFileExt: ".SEG",
		SegmentSize:    32 * 1024 * 1024,
	}
	wal, err := Open(opts)
	require.Nil(t, err)
	defer destroyWAL(wal)

	var size = 100000
	val := strings.Repeat("wal", 512)
	for i := 0; i < size; i++ {
		_, err := wal.Write([]byte(val))
		require.Nil(t, err)
	}

	validate := func(walInner *Wal, size int) {
		var i = 0
		reader := walInner.NewReader()
		for {
			chunk, position, err := reader.Next()
			if err != nil {
				if err == io.EOF {
					break
				}
				panic(err)
			}
			require.NotNil(t, chunk)
			require.NotNil(t, position)
			require.Equal(t, position.SegmentId, reader.CurrentSegmentId())
			i++
		}
		require.Equal(t, i, size)
	}

	validate(wal, size)
	err = wal.Close()
	require.Nil(t, err)

	wal2, err := Open(opts)
	require.Nil(t, err)
	defer func() {
		_ = wal2.Close()
	}()
	validate(wal2, size)
}

func testWriteAllIterate(t *testing.T, wal *Wal, size, valueSize int) {
	for i := 0; i < size; i++ {
		val := strings.Repeat("wal", valueSize)
		wal.AddPendingWrites([]byte(val))
	}
	positions, err := wal.WriteAll()
	require.Nil(t, err)
	require.Equal(t, len(positions), size)

	count := 0
	reader := wal.NewReader()
	for {
		data, pos, err := reader.Next()
		if err != nil {
			break
		}
		require.Equal(t, strings.Repeat("wal", valueSize), string(data))

		require.Equal(t, positions[count].SegmentId, pos.SegmentId)
		require.Equal(t, positions[count].BlockNumber, pos.BlockNumber)
		require.Equal(t, positions[count].ChunkOffset, pos.ChunkOffset)

		count++
	}
	require.Equal(t, len(wal.pendingWrites), 0)
}

func testWriteAndIterate(t *testing.T, wal *Wal, size int, valueSize int) {
	val := strings.Repeat("wal", valueSize)
	positions := make([]*ChunkPosition, size)
	for i := 0; i < size; i++ {
		pos, err := wal.Write([]byte(val))
		require.Nil(t, err)
		positions[i] = pos
	}

	var count int
	// iterates all the data
	reader := wal.NewReader()
	for {
		data, pos, err := reader.Next()
		if err != nil {
			break
		}
		require.Equal(t, val, string(data))

		require.Equal(t, positions[count].SegmentId, pos.SegmentId)
		require.Equal(t, positions[count].BlockNumber, pos.BlockNumber)
		require.Equal(t, positions[count].ChunkOffset, pos.ChunkOffset)

		count++
	}
	require.Equal(t, size, count)
}

func TestWAL_Delete(t *testing.T) {
	dir, _ := os.MkdirTemp("", "wal-test-delete")
	opts := Options{
		DirPath:        dir,
		SegmentFileExt: ".SEG",
		SegmentSize:    32 * 1024 * 1024,
	}
	wal, err := Open(opts)
	require.Nil(t, err)
	testWriteAndIterate(t, wal, 2000, 512)
	require.False(t, wal.IsEmpty())
	defer destroyWAL(wal)

	err = wal.Delete()
	require.Nil(t, err)

	wal, err = Open(opts)
	require.Nil(t, err)
	require.True(t, wal.IsEmpty())
}

func TestWAL_ReaderWithStart(t *testing.T) {
	dir, _ := os.MkdirTemp("./", "wal-test-wal-reader-with-start")
	opts := Options{
		DirPath:        dir,
		SegmentFileExt: ".SEG",
		SegmentSize:    8 * 1024 * 1024,
	}
	wal, err := Open(opts)
	require.Nil(t, err)
	defer destroyWAL(wal)

	_, err = wal.NewReaderWithStart(nil)
	require.NotNil(t, err)

	reader1, err := wal.NewReaderWithStart(&ChunkPosition{SegmentId: 0, BlockNumber: 0, ChunkOffset: 100})
	require.Nil(t, err)
	_, _, err = reader1.Next()
	require.Equal(t, err, io.EOF)

	testWriteAndIterate(t, wal, 20000, 512)
	reader2, err := wal.NewReaderWithStart(&ChunkPosition{SegmentId: 0, BlockNumber: 0, ChunkOffset: 0})
	require.Nil(t, err)
	_, pos2, err := reader2.Next()
	require.Nil(t, err)
	require.Equal(t, pos2.BlockNumber, uint32(0))
	require.Equal(t, pos2.ChunkOffset, int64(0))

	reader3, err := wal.NewReaderWithStart(&ChunkPosition{SegmentId: 3, BlockNumber: 5, ChunkOffset: 0})
	require.Nil(t, err)
	_, pos3, err := reader3.Next()
	require.Nil(t, err)
	require.Equal(t, pos3.SegmentId, uint32(3))
	require.Equal(t, pos3.BlockNumber, uint32(5))
}

func TestWAL_RenameFileExt(t *testing.T) {
	dir, _ := os.MkdirTemp("", "wal-test-rename-ext")
	opts := Options{
		DirPath:        dir,
		SegmentFileExt: ".VLOG.1.temp",
		SegmentSize:    8 * 1024 * 1024,
	}
	wal, err := Open(opts)
	require.Nil(t, err)
	defer destroyWAL(wal)
	testWriteAndIterate(t, wal, 20000, 512)

	err = wal.Close()
	require.Nil(t, err)

	err = wal.RenameFileExt(".VLOG.1")
	require.Nil(t, err)

	opts.SegmentFileExt = ".VLOG.1"
	wal2, err := Open(opts)
	require.Nil(t, err)
	defer func() {
		_ = wal2.Close()
	}()
	for i := 0; i < 20000; i++ {
		_, err = wal2.Write([]byte(strings.Repeat("W", 512)))
		require.Nil(t, err)
	}
}
