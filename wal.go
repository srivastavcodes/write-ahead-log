package wal

import (
	"errors"
	"fmt"
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
	olderSegment      map[SegmentId]*segment // older segment files, used only for reads.
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
		olderSegment:  make(map[SegmentId]*segment),
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
				wal.olderSegment[seg.id] = seg
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
