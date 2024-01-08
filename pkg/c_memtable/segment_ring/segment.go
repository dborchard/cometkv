package segment_ring

import (
	"cometkv/pkg/common"
	"cometkv/pkg/common/timestamp"
	"container/list"
	"context"
	"github.com/alphadose/zenq/v2"
	"runtime"
	"sync/atomic"
	"time"
)

type Segment struct {
	// Original Structure
	tree *BTreeGCoW[common.Pair[[]byte, *list.Element]]
	vlog *list.List
	ctx  context.Context

	// Ring Enhancement
	nextPtr *Segment

	// Async Logic
	asyncKeyPtrChan *zenq.ZenQ[*common.Pair[[]byte, *list.Element]]
	done            atomic.Bool
	pendingUpdates  atomic.Int64
}

type ISegment interface {
	AddValue(val []byte) (lePtr *list.Element)
	AddIndex(entry *common.Pair[[]byte, *list.Element])
	AddIndexAsync(entry *common.Pair[[]byte, *list.Element])

	Scan(startKey string, count int, snapshotTs time.Time) []common.Pair[string, []byte]
	Free() int

	Len() int
}

func NewSegment(ctx context.Context) *Segment {
	segment := Segment{
		tree: NewBTreeGCoW(func(a, b common.Pair[[]byte, *list.Element]) bool {
			return common.CompareKeys(a.Key, b.Key) < 0
		}),
		vlog:            list.New(),
		ctx:             ctx,
		asyncKeyPtrChan: zenq.New[*common.Pair[[]byte, *list.Element]](1 << 20),
		done:            atomic.Bool{},
	}

	return &segment
}

func (s *Segment) StartListener() {
	// Ctx Listener
	go func() {
		for {
			select {
			case <-s.ctx.Done():
				s.done.Store(true)
				return
			}
		}
	}()

	// Writer Thread
	go func() {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()

		var entry *common.Pair[[]byte, *list.Element]
		var isQueueOpen bool

		defer s.asyncKeyPtrChan.Close()
		for {

			if entry, isQueueOpen = s.asyncKeyPtrChan.Read(); isQueueOpen {
				s.tree.Set(*entry)
				s.pendingUpdates.Add(-1)
			}

			if s.done.Load() {
				return
			}
		}
	}()
}

func (s *Segment) AddValue(val []byte) (lePtr *list.Element) {
	return s.vlog.PushFront(val)
}

func (s *Segment) AddIndex(entry *common.Pair[[]byte, *list.Element]) {
	// For explicit serialization
	delay := time.Duration(1)
	for s.pendingUpdates.Load() > 0 {
		// Waiting time was generally between 10-250ms
		time.Sleep(delay * time.Millisecond)
		delay = delay * 2
	}

	s.tree.Set(*entry)
}

func (s *Segment) AddIndexAsync(entry *common.Pair[[]byte, *list.Element]) {
	s.asyncKeyPtrChan.Write(entry)
	s.pendingUpdates.Add(1)
}

func (s *Segment) Scan(startKey string, count int, snapshotTs time.Time) []common.Pair[string, []byte] {
	delay := time.Duration(1)
	for s.pendingUpdates.Load() > 0 {
		// Waiting time was generally between 10-250ms
		time.Sleep(delay * time.Millisecond)
		delay = delay * 2
	}

	snapshotTsNano := timestamp.ToUnit64(snapshotTs)

	// 1. Do range scan
	internalKey := common.KeyWithTs([]byte(startKey), timestamp.ToUnit64(snapshotTs))
	startRow := common.Pair[[]byte, *list.Element]{Key: internalKey}
	uniqueKVs := make(map[string][]byte)
	seenKeys := make(map[string]any)

	idx := 1
	s.tree.Ascend(startRow, func(item common.Pair[[]byte, *list.Element]) bool {
		if idx > count {
			return false
		}

		// expiredTs < ItemTs < snapshotTs
		itemTs := common.ParseTs(item.Key)
		lessThanOrEqualToSnapshotTs := itemTs <= snapshotTsNano

		if lessThanOrEqualToSnapshotTs {
			strKey := string(common.ParseKey(item.Key))
			if _, seen := seenKeys[strKey]; !seen {
				seenKeys[strKey] = true
				if item.Val != nil && item.Val.Value.([]byte) != nil {
					uniqueKVs[strKey] = item.Val.Value.([]byte)
					idx++
				}
			}
		}
		return true
	})

	return common.MapToArray(uniqueKVs)
}

func (s *Segment) Free() int {
	//NOTE: DO NOT CLOSE WRITER THREAD HERE.
	removedCount := s.Len()

	s.tree.Clear()
	s.vlog.Init()

	return removedCount
}

func (s *Segment) Len() int {
	return s.tree.Len()
}
