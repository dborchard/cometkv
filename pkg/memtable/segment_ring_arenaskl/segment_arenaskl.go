package segment_ring_arenaskl

import (
	"context"
	"github.com/alphadose/zenq/v2"
	"github.com/dborchard/cometkv/pkg/memtable/mor_arenaskl/arenaskl"
	"github.com/dborchard/cometkv/pkg/y/entry"
	"github.com/dborchard/cometkv/pkg/y/timestamp"
	"runtime"
	"sync/atomic"
	"time"
)

const (
	MaxArenaSize = 1 << 20
)

type Segment struct {
	// Original Structure
	tree *arenaskl.Skiplist
	vlog *arenaskl.Arena
	ctx  context.Context

	// Ring Enhancement
	nextPtr *Segment

	// Async Logic
	asyncKeyPtrChan *zenq.ZenQ[*entry.Pair[[]byte, uint64]]
	done            atomic.Bool
	pendingUpdates  atomic.Int64
}

type ISegment interface {
	AddValue(val []byte) (lePtr uint64)
	AddIndex(entry *entry.Pair[[]byte, uint64])
	AddIndexAsync(entry *entry.Pair[[]byte, uint64])

	Scan(startKey string, count int, snapshotTs time.Time) []entry.Pair[string, []byte]
	Free() int

	Len() int
}

func NewSegment(ctx context.Context) *Segment {
	segment := Segment{
		tree:            arenaskl.NewSkiplist(arenaskl.NewArena(MaxArenaSize)),
		vlog:            arenaskl.NewArena(MaxArenaSize),
		ctx:             ctx,
		asyncKeyPtrChan: zenq.New[*entry.Pair[[]byte, uint64]](1 << 20),
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

		var entry *entry.Pair[[]byte, uint64]
		var isQueueOpen bool

		var it arenaskl.Iterator
		it.Init(s.tree)
		defer s.asyncKeyPtrChan.Close()
		for {

			if entry, isQueueOpen = s.asyncKeyPtrChan.Read(); isQueueOpen {
				_ = it.Add(entry.Key, EncodeUint64(&entry.Val), 0)
				s.pendingUpdates.Add(-1)
			}

			if s.done.Load() {
				return
			}
		}
	}()
}

func (s *Segment) AddValue(val []byte) (lePtr uint64) {
	offset, err := s.vlog.Alloc(uint32(len(val)), 0, arenaskl.Align1)
	if err != nil {
		panic("value is too large")
	}

	copy(s.vlog.GetBytes(offset, uint32(len(val))), val)
	return arenaskl.EncodeValue(offset, uint16(len(val)), 0)
}

func (s *Segment) AddIndex(entry *entry.Pair[[]byte, uint64]) {
	// For explicit serialization
	delay := time.Duration(1)
	for s.pendingUpdates.Load() > 0 {
		// Waiting time was generally between 10-250ms
		time.Sleep(delay * time.Millisecond)
		delay = delay * 2
	}
	var it arenaskl.Iterator
	it.Init(s.tree)
	_ = it.Add(entry.Key, EncodeUint64(&entry.Val), 0)
}

func (s *Segment) AddIndexAsync(entry *entry.Pair[[]byte, uint64]) {
	s.asyncKeyPtrChan.Write(entry)
	s.pendingUpdates.Add(1)
}

func (s *Segment) Scan(startKey string, count int, snapshotTs time.Time) []entry.Pair[string, []byte] {
	delay := time.Duration(1)
	for s.pendingUpdates.Load() > 0 {
		// Waiting time was generally between 10-250ms
		time.Sleep(delay * time.Millisecond)
		delay = delay * 2
	}

	snapshotTsNano := timestamp.ToUnit64(snapshotTs)

	// 1. Do range scan
	internalKey := entry.KeyWithTs([]byte(startKey), timestamp.ToUnit64(snapshotTs))
	uniqueKVs := make(map[string][]byte)
	seenKeys := make(map[string]any)

	idx := 1
	var it arenaskl.Iterator
	it.Init(s.tree)
	it.Seek(internalKey)
	var offsetUint64 uint64
	var valOffset uint32
	var valSize uint16
	for it.Valid() {
		if idx > count {
			break
		}

		itemTs := entry.ParseTs(it.Key())
		lessThanOrEqualToSnapshotTs := itemTs <= snapshotTsNano

		if lessThanOrEqualToSnapshotTs {
			strKey := string(entry.ParseKey(it.Key()))
			if _, seen := seenKeys[strKey]; !seen {
				seenKeys[strKey] = true
				if it.Value() != nil {
					offsetUint64 = DecodeUint64(it.Value())
					valOffset, valSize = arenaskl.DecodeValue(offsetUint64)
					uniqueKVs[strKey] = s.vlog.GetBytes(valOffset, uint32(valSize))
					idx++
				}
			}
		}
		it.Next()
	}

	return entry.MapToArray(uniqueKVs)
}

func (s *Segment) Free() int {
	//NOTE: DO NOT CLOSE WRITER THREAD HERE.
	removedCount := s.Len()

	//s.tree.Reset()
	s.vlog.Reset()

	return removedCount
}

func (s *Segment) Len() int {
	total := 0
	var it arenaskl.Iterator
	it.Init(s.tree)
	for it.SeekToFirst(); it.Valid(); it.Next() {
		total++
	}

	return total
}
