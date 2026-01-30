package wal

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

const initialSegmentFileId = 1

var (
	ErrValueTooLarge       = errors.New("the data size can't be larger than segment size")
	ErrPendingSizeTooLarge = errors.New("the upper bound of pending writes can't be larger than segment size")
)

// Wal represents a Write-Ahead Log structure that provides durability
// and fault-tolerance for incoming writes.
// It consists of an active segment, which is the current segment file
// used for new incoming writes; and older segments, which are a map of
// segment files used to read operations.
//
// The options field stores various configuration options for the wal.
// Wal is safe for concurrent operations (access and modifications).
type Wal struct {
	olderSegments     map[SegmentId]*segment // older segment files, used only for reads.
	activeSegment     *segment               // active segment, used for incoming writes.
	options           Options
	mu                sync.RWMutex
	bytesWritten      uint32
	renameIds         []SegmentId
	pendingWrites     [][]byte
	pendingSize       int64
	pendingWritesLock sync.Mutex
	closech           chan struct{}
	syncTicker        *time.Ticker
}

// Reader represents a reader for the Wal. It consists of segmentReaders, which is a
// slice of segmentReader sorted by segmentId; and currentReader, which is the index
// of the current segment reader in the slice.
//
// The currentReader field is used to iterate over the segmentReader slice.
type Reader struct {
	segmentReaders []*segmentReader
	currentReader  int
}

// Open opens a Wal with the provided options. It opens all the segment files in the
// directory or creates a directory if not exists. If there is no segment file in
// the directory, it will create a new one.
func Open(opt Options) (*Wal, error) {
	if !strings.HasPrefix(opt.SegmentFileExt, ".") {
		return nil, errors.New("segment file extension must start with '.'")
	}
	wal := &Wal{
		options:       opt,
		olderSegments: make(map[SegmentId]*segment),
		pendingWrites: make([][]byte, 0),
		closech:       make(chan struct{}),
	}
	// create the dir if not exists.
	err := os.MkdirAll(opt.DirPath, os.ModePerm)
	if err != nil {
		return nil, err
	}
	// iterate the dir and open all segment files.
	entries, err := os.ReadDir(opt.DirPath)
	if err != nil {
		return nil, err
	}
	var segmentIds []int

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		var id int
		_, err := fmt.Sscanf(entry.Name(), "%d"+opt.SegmentFileExt, &id)
		if err != nil {
			continue
		}
		segmentIds = append(segmentIds, id)
	}
	// if empty directory, just initialize a new segment file.
	if len(segmentIds) == 0 {
		seg, err := openSegment(opt.DirPath, opt.SegmentFileExt, initialSegmentFileId)
		if err != nil {
			return nil, fmt.Errorf("error initializing the first segment: %w", err)
		}
		wal.activeSegment = seg
	} else {
		// open the segment files in order and get the max one as the active
		// segment file.
		sort.Ints(segmentIds)

		for i, segId := range segmentIds {
			seg, err := openSegment(opt.DirPath, opt.SegmentFileExt, uint32(segId))
			if err != nil {
				return nil, fmt.Errorf("error opening segment %d: %w", i, err)
			}
			if i == len(segmentIds)-1 {
				wal.activeSegment = seg
			} else {
				wal.olderSegments[seg.id] = seg
			}
		}
	}
	// only start the sync operation if the sync interval is greater than 0.
	if wal.options.SyncInterval > 0 {
		wal.syncTicker = time.NewTicker(wal.options.SyncInterval)
		go func() {
			for {
				select {
				case <-wal.syncTicker.C:
					_ = wal.Sync()
				case <-wal.closech:
					wal.syncTicker.Stop()
					return
				}
			}
		}()
	}
	return wal, nil
}

// Sync syncs the active segment file to stable storage like disk.
func (wal *Wal) Sync() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	return wal.activeSegment.Sync()
}

// OpenNewActiveSegment opens a new segment file and sets it as the active segment.
// It is used when even though the active segment is not full yet, the user want to
// create a new segment file.
func (wal *Wal) OpenNewActiveSegment() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	// sync the active segment (write it to disk)
	if err := wal.activeSegment.Sync(); err != nil {
		return fmt.Errorf("error syncing the active segment: %w", err)
	}
	// create a new segment that'll become active
	seg, err := openSegment(
		wal.options.DirPath,
		wal.options.SegmentFileExt, wal.activeSegment.id+1,
	)
	if err != nil {
		return fmt.Errorf("error opening new active segment: %w", err)
	}
	wal.olderSegments[wal.activeSegment.id] = wal.activeSegment
	wal.activeSegment = seg
	return nil
}

// ActiveSegmentId returns the id of the active segment file.
func (wal *Wal) ActiveSegmentId() SegmentId {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	return wal.activeSegment.id
}

// IsEmpty returns whether the Wal is empty. The Wal is considered empty if there
// are 0 older segments and 1 empty active segment.
func (wal *Wal) IsEmpty() bool {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	return len(wal.olderSegments) == 0 && wal.activeSegment.Size() == 0
}

// SetIsStartupTraversal is only used if the Wal is during its startup traversal.
// When enabled, Wal reads are NOT thread-safe and must be performed by a single
// reader goroutine only.
func (wal *Wal) SetIsStartupTraversal(val bool) {
	for _, s := range wal.olderSegments {
		s.isStartupTraversal = val
	}
	wal.activeSegment.isStartupTraversal = val
}

// NewReaderWithMax returns a new reader for the Wal, and the reader will only
// read the data from the segment file whose id is less than or equal to the
// given segId.
func (wal *Wal) NewReaderWithMax(segId SegmentId) *Reader {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	var segReaders []*segmentReader

	for _, s := range wal.olderSegments {
		if segId == 0 || s.id <= segId {
			reader := s.NewReader()
			segReaders = append(segReaders, reader)
		}
	}
	if segId == 0 || wal.activeSegment.id <= segId {
		reader := wal.activeSegment.NewReader()
		segReaders = append(segReaders, reader)
	}
	sort.Slice(segReaders, func(i, j int) bool {
		return segReaders[i].segment.id < segReaders[j].segment.id
	})
	return &Reader{
		segmentReaders: segReaders, currentReader: 0,
	}
}

// NewReader returns a new reader for the Wal. It will iterate all segment files
// and read all the data from them.
func (wal *Wal) NewReader() *Reader {
	return wal.NewReaderWithMax(0)
}

// NewReaderWithStart returns a new reader for the Wal, and the reader will only
// read the data from the segment file whose position is greater than or equal
// to the given position.
func (wal *Wal) NewReaderWithStart(startPos *ChunkPosition) (*Reader, error) {
	if startPos == nil {
		return nil, errors.New("start position cannot be nil")
	}
	wal.mu.Lock()
	defer wal.mu.Unlock()

	reader := wal.NewReader()
	for {
		// skip the segment readers whose id is less than the given position's
		// segment id.
		if reader.CurrentSegmentId() < startPos.SegmentId {
			reader.SkipCurrentSegment()
			continue
		}
		currPos := reader.CurrentChunkPosition()
		// skip chunk whose position is less than the given position.
		if startPos.BlockNumber <= currPos.BlockNumber &&
			startPos.ChunkOffset <= currPos.ChunkOffset {
			break
		}
		// advance reader until currPos reaches startPos.
		if _, _, err := reader.Next(); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
	}
	return reader, nil
}

// Next returns the next chunk data and its ChunkPosition in the Wal. If there is
// no data io.EOF is returned.
//
// The position can be used to read the data from the segment file.
func (r *Reader) Next() ([]byte, *ChunkPosition, error) {
	if r.currentReader >= len(r.segmentReaders) {
		return nil, nil, io.EOF
	}
	data, chunkPos, err := r.segmentReaders[r.currentReader].Next()
	if errors.Is(err, io.EOF) {
		r.currentReader++
		return r.Next()
	}
	return data, chunkPos, err
}

// SkipCurrentSegment skips the current segment while reading the Wal.
func (r *Reader) SkipCurrentSegment() {
	r.currentReader++
}

// CurrentSegmentId returns the id of the current segment file while reading
// from the Wal.
func (r *Reader) CurrentSegmentId() SegmentId {
	return r.segmentReaders[r.currentReader].segment.id
}

// CurrentChunkPosition returns the position of the current data chunk.
func (r *Reader) CurrentChunkPosition() *ChunkPosition {
	reader := r.segmentReaders[r.currentReader]
	return &ChunkPosition{
		SegmentId:   reader.segment.id,
		ChunkOffset: reader.chunkOffset,
		BlockNumber: reader.blockNumber,
	}
}

// ClearPendingWrites clears pending writes and resets pending size.
func (wal *Wal) ClearPendingWrites() {
	wal.pendingWritesLock.Lock()
	defer wal.pendingWritesLock.Unlock()

	wal.pendingSize = 0
	wal.pendingWrites = wal.pendingWrites[:0]
}

// AddPendingWrites appends data to the pending batch.
// Size limits are enforced when the batch is flushed.
func (wal *Wal) AddPendingWrites(data []byte) {
	wal.pendingWritesLock.Lock()
	defer wal.pendingWritesLock.Unlock()

	size := wal.maxDataWriteSize(int64(len(data)))
	wal.pendingSize += size
	wal.pendingWrites = append(wal.pendingWrites, data)
}

// rotateActiveSegment creates a new segment file to replace the activeSegment.
func (wal *Wal) rotateActiveSegment() error {
	if err := wal.activeSegment.Sync(); err != nil {
		return fmt.Errorf("error syncing the active segment: %w", err)
	}
	seg, err := openSegment(
		wal.options.DirPath,
		wal.options.SegmentFileExt, wal.activeSegment.id+1,
	)
	if err != nil {
		return fmt.Errorf("error opening active segment: %w", err)
	}
	wal.olderSegments[wal.activeSegment.id] = wal.activeSegment
	wal.activeSegment = seg
	return nil
}

// WriteAll write wal.pendingWrites to Wal and clears pending writes. It will not
// sync the segment files according to wal.options, you'll have to call sync
// manually.
func (wal *Wal) WriteAll() ([]*ChunkPosition, error) {
	if len(wal.pendingWrites) == 0 {
		return make([]*ChunkPosition, 0), nil
	}
	wal.mu.Lock()
	defer func() {
		wal.ClearPendingWrites()
		wal.mu.Unlock()
	}()
	// if the pending size is large an entire segment size, return error
	if wal.pendingSize > wal.options.SegmentSize {
		return nil, ErrPendingSizeTooLarge
	}
	// if the pending size is larger than the current segment, sync it
	// and create a new one.
	if wal.activeSegment.Size()+wal.pendingSize > wal.options.SegmentSize {
		if err := wal.rotateActiveSegment(); err != nil {
			return nil, err
		}
	}
	// write all the data to the active segment file.
	return wal.activeSegment.writeAll(wal.pendingWrites)
}

// Write writes the given data to the active segment of the Wal. It returns the
// position of the data in the Wal, and an error if any.
func (wal *Wal) Write(data []byte) (*ChunkPosition, error) {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	if int64(len(data))+chunkHeaderSize > wal.options.SegmentSize {
		return nil, ErrValueTooLarge
	}
	// if the active segment file is full, sync it and create a new one.
	if wal.IsFull(int64(len(data))) {
		if err := wal.rotateActiveSegment(); err != nil {
			return nil, err
		}
	}
	position, err := wal.activeSegment.Write(data)
	if err != nil {
		return nil, err
	}
	wal.bytesWritten += position.ChunkSize
	// sync the active segment if needed
	isSync := wal.options.Sync

	if !isSync && wal.options.BytesPerSync > 0 {
		isSync = wal.bytesWritten >= wal.options.BytesPerSync
	}
	if isSync {
		if err = wal.activeSegment.Sync(); err != nil {
			return nil, err
		}
		wal.bytesWritten = 0
	}
	return position, nil
}

// Read reads the data from the Wal according to the given position.
func (wal *Wal) Read(pos *ChunkPosition) ([]byte, error) {
	wal.mu.RLock()
	defer wal.mu.RUnlock()

	var seg *segment
	if wal.activeSegment.id == pos.SegmentId {
		seg = wal.activeSegment
	} else {
		seg = wal.olderSegments[pos.SegmentId]
	}
	if seg == nil {
		return nil, fmt.Errorf(
			"segment %d%s not found",
			pos.SegmentId, wal.options.SegmentFileExt,
		)
	}
	return seg.Read(pos.BlockNumber, pos.ChunkOffset)
}

// Close closes all the older segment files and the current active segment file.
func (wal *Wal) Close() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	select {
	case <-wal.closech: // channel already closed
	default:
		close(wal.closech)
	}
	for id, s := range wal.olderSegments {
		if err := s.Close(); err != nil {
			return err
		}
		wal.renameIds = append(wal.renameIds, id)
	}
	wal.olderSegments = nil

	err := wal.activeSegment.Close()
	if err != nil {
		return err
	}
	wal.renameIds = append(wal.renameIds, wal.activeSegment.id)
	return nil
}

// Delete deletes all the older segment files and the current active segment file.
func (wal *Wal) Delete() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	for _, s := range wal.olderSegments {
		if err := s.Remove(); err != nil {
			return err
		}
	}
	wal.olderSegments = nil
	return wal.activeSegment.Remove()
}

// RenameFileExt renames all segment files' extension name and replaces the global
// Options.SegmentFileExt configuration for this Wal instance.
func (wal *Wal) RenameFileExt(ext string) error {
	if !strings.HasPrefix(ext, ".") {
		return errors.New("segment file extension must start with '.'")
	}
	wal.mu.Lock()
	defer wal.mu.Unlock()

	renameFileFn := func(id SegmentId) error {
		oldName := SegmentFileName(wal.options.DirPath, wal.options.SegmentFileExt, id)
		newName := SegmentFileName(wal.options.DirPath, ext, id)
		return os.Rename(oldName, newName)
	}
	for _, id := range wal.renameIds {
		if err := renameFileFn(id); err != nil {
			return err
		}
	}
	wal.options.SegmentFileExt = ext
	return nil
}

// IsFull checks if the active segment file can hold the incoming data or not.
func (wal *Wal) IsFull(size int64) bool {
	return wal.activeSegment.Size()+
		wal.maxDataWriteSize(size) > wal.options.SegmentSize
}

// maxDataWriteSize calculates the possible maximum size. The maximum size is:
//   - single header size + data size + max padding ((num_blocks + 1) * header size)
func (wal *Wal) maxDataWriteSize(size int64) int64 {
	return chunkHeaderSize + size + (size/blockSize+1)*chunkHeaderSize
}
