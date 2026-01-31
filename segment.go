package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"wal/bufpool"
)

type SegmentId = uint32
type ChunkType = byte

const (
	ChunkTypeFull ChunkType = iota
	ChunkTypeFirst
	ChunkTypeMiddle
	ChunkTypeLast
)

var (
	ErrClosed       = errors.New("the segment file is closed")
	ErrInvalidCrc32 = errors.New("invalid CRC, the data may be corrupted")
)

const (
	fileModePerm = 0644

	// Checksum = 4 + Length = 2 + Type = 1
	chunkHeaderSize = 7

	blockSize = 32 * KB

	// SegmentId  BlockNumber ChunkOffset ChunkSize
	//   uint32  +  uint32   +   int64   +  uint32
	maxLen = binary.MaxVarintLen32*3 + binary.MaxVarintLen64
)

// segment represents a single segment file in the wal. The segment file
// is append-only, and the data is written in blocks. Each block is 32KB
// and the data is written in chunks.
type segment struct {
	id SegmentId
	fd *os.File

	currentBlockNumber uint32
	currentBlockSize   uint32

	closed bool
	header []byte

	startupBlock *startupBlock

	isStartupTraversal bool
}

// segmentReader is used to iterate over all the data from the segment file.
// You can call Next to get the next chunk of data, and an io.EOF will be
// returned when there is no data.
type segmentReader struct {
	segment     *segment
	chunkOffset int64
	blockNumber uint32
}

// There is only one reader (single goroutine) for startup traversal, so we can
// use one block to finish the whole traversal to avoid memory allocation.
type startupBlock struct {
	blockNumber int64
	block       []byte
}

// ChunkPosition represent the position of a chunk in the segment file. It is
// used to read the data from the segment file.
type ChunkPosition struct {
	SegmentId   SegmentId
	ChunkOffset int64  // The start offset of the chunk in the segment file.
	ChunkSize   uint32 // Number of bytes the chunk data holds up in the segment file.
	BlockNumber uint32 // The block number of the chunk in the segment file.
}

var blockPool = sync.Pool{
	New: func() any {
		b := make([]byte, blockSize)
		return &b
	},
}

// getBlockBuf returns a pointer to a byte slice from the buffer pool.
// The returned slice has length 0 and can be appended to.
func getBlockBuf() *[]byte {
	b := blockPool.Get().(*[]byte)
	return b
}

// putBlockBuf returns a byte slice to the buffer pool for reuse.
// If the slice capacity is too large, it is reset to a smaller
// buffer to avoid retaining excessive memory.
func putBlockBuf(b *[]byte) {
	if cap(*b) > 256*1024 {
		*b = make([]byte, blockSize)
	}
	blockPool.Put(b)
}

// openSegment opens a new segment.
func openSegment(dirPath, extName string, id uint32) (*segment, error) {
	fd, err := os.OpenFile(
		SegmentFileName(dirPath, extName, id),
		os.O_CREATE|os.O_RDWR|os.O_APPEND, fileModePerm,
	)
	if err != nil {
		return nil, err
	}
	// set the current block number and block size
	offset, err := fd.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, fmt.Errorf("seek to the end of segment file %d%s failed: %v", id, extName, err)
	}
	seg := &segment{
		id:                 id,
		fd:                 fd,
		header:             make([]byte, chunkHeaderSize),
		currentBlockNumber: uint32(offset / blockSize),
		currentBlockSize:   uint32(offset % blockSize),
		startupBlock: &startupBlock{
			block:       make([]byte, blockSize),
			blockNumber: -1,
		},
		isStartupTraversal: false,
	}
	return seg, nil
}

// SegmentFileName will return the filename of a segment file.
func SegmentFileName(dirPath, extName string, id uint32) string {
	return filepath.Join(dirPath, fmt.Sprintf("%09d%s", id, extName))
}

// NewReader creates a new segment reader. You can call Next to get the next
// chunk of data, and io.EOF will be returned when there is no data.
func (s *segment) NewReader() *segmentReader {
	return &segmentReader{
		blockNumber: 0,
		segment:     s,
		chunkOffset: 0,
	}
}

// Sync flushes the segment file to disk.
func (s *segment) Sync() error {
	if s.closed {
		return nil
	}
	return s.fd.Sync()
}

// Remove removes the segment file. Errs if file cannot be closed.
func (s *segment) Remove() error {
	if !s.closed {
		if err := s.fd.Close(); err != nil {
			return err
		}
		s.closed = true
	}
	return os.Remove(s.fd.Name())
}

// Close closes the segment file.
func (s *segment) Close() error {
	if s.closed {
		return nil
	}
	if err := s.fd.Close(); err != nil {
		return err
	}
	s.closed = true
	return nil
}

// Size returns the size of the segment file.
func (s *segment) Size() int64 {
	size := int64(s.currentBlockNumber) * int64(blockSize)
	return size + int64(s.currentBlockSize)
}

// Write writes the data to the buffer and then to the segment file. It returns
// the ChunkPosition for the data written to the segment file.
func (s *segment) Write(data []byte) (pos *ChunkPosition, err error) {
	if s.closed {
		return nil, ErrClosed
	}
	var (
		originalBlockNumber = s.currentBlockNumber
		originalBlockSize   = s.currentBlockSize
	)
	chunkBuffer := bufpool.GetFromBuf()
	defer func() {
		if err != nil {
			s.currentBlockNumber = originalBlockNumber
			s.currentBlockSize = originalBlockSize
		}
		bufpool.PutIntoBuf(chunkBuffer)
	}()
	pos, err = s.writeToBuffer(data, chunkBuffer)
	if err != nil {
		return nil, err
	}
	if err = s.writeChunkBuffer(chunkBuffer); err != nil {
		return nil, err
	}
	return
}

// writeToBuffer calculates the chunk position for data, writes data to bufferPool,
// and updates segment status. The data will be written in chunks, and the chunks
// have four types:
// ChunkTypeFull, ChunkTypeFirst, ChunkTypeMiddle, ChunkTypeLast.
//
// Each chunk has a header and payload, and the header contains the checksum, length,
// and type. The payload of the chunk is the real data you want to write.
func (s *segment) writeToBuffer(data []byte, chunkBuffer *[]byte) (*ChunkPosition, error) {
	var (
		startBufferLen = len(*chunkBuffer)
		padding        = uint32(0)
	)
	if s.closed {
		return nil, ErrClosed
	}
	// if the left block size cannot hold the chunk header, pad the block
	if s.currentBlockSize+chunkHeaderSize >= blockSize {
		// padding if necessary
		if s.currentBlockSize < blockSize {
			pad := make([]byte, blockSize-s.currentBlockSize)
			*chunkBuffer = append(*chunkBuffer, pad...)
			padding += blockSize - s.currentBlockSize
		}
		// start from a new block
		s.currentBlockSize = 0
		s.currentBlockNumber += 1
	}
	// return the start position of the chunk that the user can use to read the data.
	pos := &ChunkPosition{
		SegmentId:   s.id,
		BlockNumber: s.currentBlockNumber,
		ChunkOffset: int64(s.currentBlockSize),
	}
	dataSize := uint32(len(data))
	// the entire chunk can fit into the block.
	if s.currentBlockSize+dataSize+chunkHeaderSize <= blockSize {
		s.appendChunkBuffer(chunkBuffer, data, ChunkTypeFull)
		pos.ChunkSize = dataSize + chunkHeaderSize
	} else {
		// if the size of the data exceeds the size of the block,
		// the data should be written to the block in batches.
		var (
			blockCount    uint32 = 0
			leftSize             = dataSize
			currBlockSize        = s.currentBlockSize
		)
		for leftSize > 0 {
			chunkSize := blockSize - currBlockSize - chunkHeaderSize
			if chunkSize > leftSize {
				chunkSize = leftSize
			}
			end := dataSize - leftSize + chunkSize
			if end > dataSize {
				end = dataSize
			}
			// append the chunks to the buffer.
			var chunkType ChunkType

			switch leftSize {
			case dataSize:
				chunkType = ChunkTypeFirst
			case chunkSize:
				chunkType = ChunkTypeLast
			default:
				chunkType = ChunkTypeMiddle
			}
			s.appendChunkBuffer(chunkBuffer, data[dataSize-leftSize:end], chunkType)

			leftSize -= chunkSize
			blockCount++

			currBlockSize = (currBlockSize + chunkSize + chunkHeaderSize) % blockSize
		}
		pos.ChunkSize = blockCount*chunkHeaderSize + dataSize
	}
	// the buffer length must be equal to chunkSize + padding length
	endBufferLen := len(*chunkBuffer)

	if pos.ChunkSize+padding != uint32(endBufferLen-startBufferLen) {
		return nil, fmt.Errorf(
			"invalid! chunk size %d is not equal to the buffer len %d",
			pos.ChunkSize+padding,
			endBufferLen-startBufferLen,
		)
	}
	// update segment status
	s.currentBlockSize += pos.ChunkSize

	if s.currentBlockSize > blockSize {
		s.currentBlockNumber += s.currentBlockSize / blockSize
		s.currentBlockSize = s.currentBlockSize % blockSize
	}
	return pos, nil
}

// writeAll writes batch data to the segment file. In case of error, the returned
// ChunkPosition may be partially filled and must be ignored for anything but
// diagnostics.
func (s *segment) writeAll(data [][]byte) (positions []*ChunkPosition, err error) {
	if s.closed {
		return nil, ErrClosed
	}
	// if any errors occur, restore the original segment status.
	var (
		originalBlockNumber = s.currentBlockNumber
		originalBlockSize   = s.currentBlockSize
	)
	// init chunk buffer
	chunkBuffer := bufpool.GetFromBuf()
	// restores original status
	defer func() {
		if err != nil {
			s.currentBlockNumber = originalBlockNumber
			s.currentBlockSize = originalBlockSize
		}
		bufpool.PutIntoBuf(chunkBuffer)
	}()
	pos := new(ChunkPosition)

	positions = make([]*ChunkPosition, len(data))
	for i := 0; i < len(positions); i++ {
		pos, err = s.writeToBuffer(data[i], chunkBuffer)
		if err != nil {
			return positions, err
		}
		positions[i] = pos
	}
	// write the chunk buffer to the segment file.
	if err = s.writeChunkBuffer(chunkBuffer); err != nil {
		return
	}
	return positions, nil
}

// appendChunkBuffer appends into the chunkBuffer the data and relevant
// headers after appending into the header - crc32 checksum of header &
// data, len of data, and the ChunkType.
//
// | CRC (4B) | Length (2B) | Type (1B) |  Payload  |
func (s *segment) appendChunkBuffer(chunkBuffer *[]byte, data []byte, chunkType ChunkType) {
	// Length: 2 bytes index:[4-5]
	binary.LittleEndian.PutUint16(s.header[4:6], uint16(len(data)))
	// Type: 1 byte index:[6]
	s.header[6] = chunkType

	sum := crc32.ChecksumIEEE(s.header[4:])
	sum = crc32.Update(sum, crc32.IEEETable, data)
	// CheckSum: 4 bytes index:[0-3]
	binary.LittleEndian.PutUint32(s.header[:4], sum)
	// append the header and data to segment chunk buffer.
	*chunkBuffer = append(*chunkBuffer, s.header...)
	*chunkBuffer = append(*chunkBuffer, data...)
}

// writeChunkBuffer writes the data in the buffer to the segment file, and
// invalidates the startupBlock cache.
func (s *segment) writeChunkBuffer(chunkBuffer *[]byte) error {
	if s.currentBlockSize > blockSize {
		return errors.New("the current block size exceeds the maximum block size")
	}
	// write the data to the underlying file.
	_, err := s.fd.Write(*chunkBuffer)
	if err != nil {
		return err
	}
	// the cached block cannot be reused again after writes.
	s.startupBlock.blockNumber = -1
	return nil
}

// Read reads the data from the segment file by the block number and chunk
// offset.
func (s *segment) Read(blockNumber uint32, chunkOffset int64) ([]byte, error) {
	value, _, err := s.readInternal(blockNumber, chunkOffset)
	return value, err
}

// readInternal reads and assembles a logical record starting at the given
// (blockNumber, chunkOffset). A record may span multiple chunks and blocks;
// the method advances across blocks until a ChunkTypeFull or ChunkTypeLast
// is encountered, which marks the end of the logical record.
//
// It returns:
//   - the reconstructed record payload
//   - the next ChunkPosition from which the following read should continue
//   - io.EOF if there is no more readable data in the segment
//
// During startup traversal, a shared startupBlock buffer may be reused to
// avoid redundant disk reads; otherwise data is read directly from disk.
// Each chunk's payload is verified using its stored checksum.
func (s *segment) readInternal(blockNumber uint32, chunkOffset int64) ([]byte, *ChunkPosition, error) {
	if s.closed {
		return nil, nil, ErrClosed
	}
	var (
		result []byte
		block  []byte

		segSize   = s.Size()
		nextChunk = &ChunkPosition{SegmentId: s.id}
	)
	if s.isStartupTraversal {
		block = s.startupBlock.block
	} else {
		block = *getBlockBuf()

		if len(block) != blockSize {
			block = make([]byte, blockSize)
		}
		defer putBlockBuf(&block)
	}
readLoop:
	for {
		var (
			blockOff = int64(blockNumber) * blockSize
			size     = int64(blockSize)
		)
		if size+blockOff > segSize {
			size = segSize - blockOff
		}
		if chunkOffset >= size {
			return nil, nil, io.EOF
		}
		if s.isStartupTraversal {
			// There are two cases where we should have to read from the disk:
			//	1. The acquired block is not the cached one.
			//    2. New writes were appended to the block, and the block is
			//       still smaller than 32KiB; hence, we must read it again
			//       because of the new writes.
			if s.startupBlock.blockNumber != int64(blockNumber) || size != blockSize {
				_, err := s.fd.ReadAt(block[0:size], blockOff)
				if err != nil {
					return nil, nil, err
				}
				// remember the block
				s.startupBlock.blockNumber = int64(blockNumber)
			}
		} else {
			_, err := s.fd.ReadAt(block[0:size], blockOff)
			if err != nil {
				return nil, nil, err
			}
		}
		var (
			header    = block[chunkOffset : chunkOffset+chunkHeaderSize] // Header
			dataLen   = binary.LittleEndian.Uint16(header[4:6])          // Length
			chunkType = header[6]                                        // Type
			startIdx  = chunkOffset + chunkHeaderSize                    // Payload
		)
		// CheckSum
		result = append(result, block[startIdx:startIdx+int64(dataLen)]...)
		var (
			checksumEndIdx = chunkOffset + chunkHeaderSize + int64(dataLen)
			currChecksum   = crc32.ChecksumIEEE(block[chunkOffset+4 : checksumEndIdx])
			savedChecksum  = binary.LittleEndian.Uint32(header[:4])
		)
		if currChecksum != savedChecksum {
			return nil, nil, ErrInvalidCrc32
		}
		// ChunkTypeFull or ChunkTypeLast marks the end of a logical record within the block.
		// The next record starts immediately after this chunk.
		// If there is no space for another header, the reader advances to the next block.
		if chunkType == ChunkTypeFull || chunkType == ChunkTypeLast {
			nextChunk.BlockNumber = blockNumber
			nextChunk.ChunkOffset = checksumEndIdx
			// If this is the last chunk in the block, and left block spaces are
			// paddings, the next chunk should be in the next block.
			if checksumEndIdx+chunkHeaderSize >= blockSize {
				nextChunk.BlockNumber += 1
				nextChunk.ChunkOffset = 0
			}
			break readLoop
		}
		blockNumber++
		chunkOffset = 0
	}
	return result, nextChunk, nil
}

// Next returns the next chunk data. You can keep calling it repeatedly until io.EOF.
func (sr *segmentReader) Next() ([]byte, *ChunkPosition, error) {
	if sr.segment.closed {
		return nil, nil, ErrClosed
	}
	resChunkPos := &ChunkPosition{
		SegmentId:   sr.segment.id,
		BlockNumber: sr.blockNumber,
		ChunkOffset: sr.chunkOffset,
	}
	value, nextChunkPos, err := sr.segment.readInternal(sr.blockNumber, sr.chunkOffset)
	if err != nil {
		return nil, nil, err
	}
	// remember that the chunk size is just an estimate, not accurate, so don't use
	// it for any important logic.
	newOff := nextChunkPos.BlockNumber*blockSize + uint32(nextChunkPos.ChunkOffset)
	oldOff := sr.blockNumber*blockSize + uint32(sr.chunkOffset)

	resChunkPos.ChunkSize = newOff - oldOff
	// update the position.
	sr.blockNumber = nextChunkPos.BlockNumber
	sr.chunkOffset = nextChunkPos.ChunkOffset

	return value, resChunkPos, nil
}

// Encode encodes the chunk position to a byte slice; returns the slice with
// the actual occupied elements.
func (cp *ChunkPosition) Encode() []byte {
	return cp.encode(true)
}

// EncodeFixedSize encodes the chunk position to a byte slice; returns a slice
// of size "maxLen".
func (cp *ChunkPosition) EncodeFixedSize() []byte {
	return cp.encode(false)
}

func (cp *ChunkPosition) encode(shrink bool) []byte {
	buf := make([]byte, maxLen)
	index := 0
	index += binary.PutUvarint(buf[index:], uint64(cp.SegmentId))
	index += binary.PutUvarint(buf[index:], uint64(cp.BlockNumber))
	index += binary.PutUvarint(buf[index:], uint64(cp.ChunkOffset))
	index += binary.PutUvarint(buf[index:], uint64(cp.ChunkSize))
	if shrink {
		return buf[:index]
	}
	return buf
}

// DecodeChunkPosition decodes the chunk position from a byte slice; returns a pointer to a
// new ChunkPosition.
func DecodeChunkPosition(buf []byte) *ChunkPosition {
	if len(buf) == 0 {
		return nil
	}
	var index int

	segmentId, n := binary.Uvarint(buf[index:])
	index += n
	blockNumber, n := binary.Uvarint(buf[index:])
	index += n
	chunkOffset, n := binary.Uvarint(buf[index:])
	index += n
	chunkSize, n := binary.Uvarint(buf[index:])
	index += n
	return &ChunkPosition{
		SegmentId: uint32(segmentId), BlockNumber: uint32(blockNumber),
		ChunkOffset: int64(chunkOffset), ChunkSize: uint32(chunkSize),
	}
}
