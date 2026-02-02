package benchmark

import (
	"bytes"
	"math/rand"
	"os"
	"testing"

	"github.com/srivastavcodes/wal"

	"github.com/stretchr/testify/require"
)

var waLog *wal.Wal

func init() {
	dir, err := os.MkdirTemp("./", "wal-bench-test")

	opt := wal.Options{
		SegmentSize:    wal.GB,
		DirPath:        dir,
		SegmentFileExt: ".SEG",
	}
	waLog, err = wal.Open(opt)

	if err != nil {
		panic(err)
	}
}

func BenchmarkWal_WriteLargeSize(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	// ensures records are not aligned properly and edge cases are touched.
	input := bytes.Repeat([]byte("log data"), 256*wal.KB+500)
	for i := 0; i < b.N; i++ {
		_, err := waLog.Write(input)
		require.NoError(b, err)
	}
}

func BenchmarkWal_WriteSmallSize(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := waLog.Write([]byte("log data"))
		require.NoError(b, err)
	}
}

func BenchmarkWal_WriteBatch(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		for j := 0; j < 50; j++ {
			waLog.AddPendingWrites(bytes.Repeat([]byte("in log data"), wal.MB))
		}
		waLog.AddPendingWrites(bytes.Repeat([]byte("out log data"), wal.MB))
		pos, err := waLog.WriteAll()
		require.NoError(b, err)
		require.Equal(b, 51, len(pos))
	}
}

func BenchmarkWal_Read(b *testing.B) {
	positions := make([]*wal.ChunkPosition, 0, 1000000)
	for i := 0; i < 1000000; i++ {
		pos, err := waLog.Write([]byte("log data"))
		require.NoError(b, err)
		positions = append(positions, pos)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := waLog.Read(positions[rand.Intn(len(positions))])
		require.NoError(b, err)
	}
}
