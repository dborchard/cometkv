package mbtree

import (
	common "cometkv/pkg/y_common"
	"cometkv/pkg/y_common/timestamp"
	"container/heap"
	"github.com/tidwall/btree"
	"time"
)

type IO struct {
	files []*btree.BTreeG[common.Pair[[]byte, []byte]]
}

func (io *IO) Get(key string, snapshotTs time.Time) []byte {
	panic("implement me")
}

func NewMBtreeIO() *IO {
	return &IO{
		files: make([]*btree.BTreeG[common.Pair[[]byte, []byte]], 0),
	}
}

func (io *IO) Scan(startKey string, count int, snapshotTs time.Time) []common.Pair[string, []byte] {
	internalKey := common.KeyWithTs([]byte(startKey), timestamp.ToUnit64(snapshotTs))
	startRow := common.Pair[[]byte, []byte]{Key: internalKey}

	//1. Init Heap
	mh := &MinHeap{}
	heap.Init(mh)

	// 2. Fetch all iterators and add to PQ
	for _, file := range io.files {
		iter := file.Copy().Iter()
		iter.Seek(startRow)
		heap.Push(mh, &iter)
	}

	// 3.a Variable
	seenKeys := make(map[string]any)
	uniqueKVs := make(map[string][]byte)
	idx := 1
	snapshotTsNano := timestamp.ToUnit64(snapshotTs)

	for mh.Len() > 0 {
		smallestIter := heap.Pop(mh).(*btree.IterG[common.Pair[[]byte, []byte]])
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
		itemTs := common.ParseTs(item.Key)
		lessThanOrEqualToSnapshotTs := itemTs <= snapshotTsNano

		if lessThanOrEqualToSnapshotTs {
			strKey := string(common.ParseKey(item.Key))
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
		heap.Pop(mh).(*btree.IterG[common.Pair[[]byte, []byte]]).Release()
	}

	// 4. Range Scan delegation
	return common.MapToArray(uniqueKVs)
}

func (io *IO) Create(records []common.Pair[string, []byte]) error {
	newFile := btree.NewBTreeG(func(a, b common.Pair[[]byte, []byte]) bool {
		return common.CompareKeys(a.Key, b.Key) < 0
	})
	for _, record := range records {
		internalKey := common.KeyWithTs([]byte(record.Key), timestamp.Now())
		newFile.Set(common.Pair[[]byte, []byte]{
			Key: internalKey,
			Val: record.Val,
		})
	}
	io.files = append(io.files, newFile)
	return nil
}

func (io *IO) Destroy() {
	io.files = nil
}
