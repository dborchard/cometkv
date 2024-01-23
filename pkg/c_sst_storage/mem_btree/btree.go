package mem_btree

import (
	"container/heap"
	"github.com/arjunsk/cometkv/pkg/y_internal/entry"
	"github.com/arjunsk/cometkv/pkg/y_internal/timestamp"
	"github.com/tidwall/btree"
	"sync"
	"time"
)

type IO struct {
	sync.Mutex
	files []*btree.BTreeG[entry.Pair[[]byte, []byte]]
}

func NewMBtreeIO() *IO {
	return &IO{
		files: make([]*btree.BTreeG[entry.Pair[[]byte, []byte]], 0),
	}
}

func (io *IO) Get(key string, snapshotTs time.Time) []byte {
	panic("implement me")
}

func (io *IO) Scan(startKey string, count int, snapshotTs time.Time) []entry.Pair[string, []byte] {
	internalKey := entry.KeyWithTs([]byte(startKey), timestamp.ToUnit64(snapshotTs))
	startRow := entry.Pair[[]byte, []byte]{Key: internalKey}

	//1. Init Heap
	mh := &MinHeap{}
	heap.Init(mh)

	// 2. Fetch all iterators and add to PQ
	io.Lock()
	for _, file := range io.files {
		iter := file.Copy().Iter()
		iter.Seek(startRow)
		heap.Push(mh, &iter)
	}
	io.Unlock()

	// 3.a Variable
	seenKeys := make(map[string]any)
	uniqueKVs := make(map[string][]byte)
	idx := 1
	snapshotTsNano := timestamp.ToUnit64(snapshotTs)

	for mh.Len() > 0 {
		smallestIter := heap.Pop(mh).(*btree.IterG[entry.Pair[[]byte, []byte]])
		item := smallestIter.Item()
		if smallestIter.Next() {
			heap.Push(mh, smallestIter)
		} else {
			smallestIter.Release()
		}

		// 3.b scan logic
		if idx > count {
			break
		}
		// ItemTs <= snapshotTs
		itemTs := entry.ParseTs(item.Key)
		lessThanOrEqualToSnapshotTs := itemTs <= snapshotTsNano

		if lessThanOrEqualToSnapshotTs {
			strKey := string(entry.ParseKey(item.Key))
			if _, seen := seenKeys[strKey]; !seen {
				seenKeys[strKey] = true
				if item.Val != nil {
					uniqueKVs[strKey] = item.Val
					idx++
				}
			}
		}
	}

	for mh.Len() > 0 {
		heap.Pop(mh).(*btree.IterG[entry.Pair[[]byte, []byte]]).Release()
	}

	// 4. Range Scan delegation
	return entry.MapToArray(uniqueKVs)
}

func (io *IO) Create(records []entry.Pair[string, []byte]) error {
	newFile := btree.NewBTreeG(func(a, b entry.Pair[[]byte, []byte]) bool {
		return entry.CompareKeys(a.Key, b.Key) < 0
	})
	for _, record := range records {
		internalKey := entry.KeyWithTs([]byte(record.Key), timestamp.Now())
		newFile.Set(entry.Pair[[]byte, []byte]{
			Key: internalKey,
			Val: record.Val,
		})
	}
	io.Lock()
	io.files = append(io.files, newFile)
	io.Unlock()
	return nil
}

func (io *IO) Destroy() {
	io.Lock()
	io.files = nil
	io.Unlock()
}
