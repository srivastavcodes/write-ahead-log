package bufpool

import "sync"

var bufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 32*1024)
		return &b
	},
}

// GetFromBuf returns a pointer to a byte slice from the buffer pool.
// The returned slice has length 0 and can be appended to.
func GetFromBuf() *[]byte {
	b := bufPool.Get().(*[]byte)
	*b = (*b)[:0]
	return b
}

// PutIntoBuf returns a byte slice to the buffer pool for reuse.
// If the slice capacity is too large, it is reset to a smaller
// buffer to avoid retaining excessive memory.
func PutIntoBuf(b *[]byte) {
	// cap guard so as to avoid having a large buffer being resued forever.
	if cap(*b) > 256*1024 {
		*b = make([]byte, 0, 32*1024)
	}
	bufPool.Put(b)
}
