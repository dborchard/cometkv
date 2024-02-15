package segment_ring

import "sync"

type List interface {
	Put(v []byte) uint64
	Get(idx uint64) []byte
	Init()
}

type BigList struct {
	data  []byte
	index []uint64
	mu    sync.RWMutex
}

func NewBigList() *BigList {
	return &BigList{
		data:  make([]byte, 0),
		index: make([]uint64, 0),
	}
}

func (l *BigList) Init() {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.data = make([]byte, 0)
	l.index = make([]uint64, 0)
}

func (l *BigList) Put(v []byte) uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()

	idx := uint64(len(l.data))
	l.data = append(l.data, v...)
	l.index = append(l.index, idx)

	return idx
}

func (l *BigList) Get(idx uint64) []byte {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if idx >= uint64(len(l.index)) {
		return nil // or some error handling
	}

	start := l.index[idx]
	var end uint64
	if idx == uint64(len(l.index))-1 {
		end = uint64(len(l.data))
	} else {
		end = l.index[idx+1]
	}

	return l.data[start:end]
}
