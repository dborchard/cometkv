package mor_arenaskl

import (
	"container/heap"
	"context"
	memtable "github.com/dborchard/cometkv/pkg/memtable"
	"github.com/dborchard/cometkv/pkg/memtable/base"
	"github.com/dborchard/cometkv/pkg/memtable/mor_arenaskl/arenaskl"
	"github.com/dborchard/cometkv/pkg/y/entry"
	"github.com/dborchard/cometkv/pkg/y/timestamp"
	"math"
	"time"
)

const (
	MaxArenaSize = math.MaxUint32
)

// MoRArenaSkl Ephemeral Copy-Ahead MV Tree
type MoRArenaSkl struct {
	base                  *base.EMBase
	segments              []*arenaskl.Skiplist
	ttlValidSegmentsCount int
	segmentDuration       time.Duration
	cycleDuration         int64
}

func New(gcInterval, ttl time.Duration, logStats bool, ctx context.Context) memtable.IMemtable {
	sr := MoRArenaSkl{segmentDuration: gcInterval}
	sr.ttlValidSegmentsCount = int(math.Ceil(float64(ttl) / float64(sr.segmentDuration)))

	// init with segments
	totalSegments := sr.ttlValidSegmentsCount + 1 + 1
	sr.cycleDuration = int64(time.Duration(float64(totalSegments) * float64(gcInterval)).Seconds())
	sr.init(totalSegments)

	sr.base = base.NewBase(&sr, gcInterval, ttl, logStats)
	go sr.StartGc(gcInterval, ctx)

	return &sr
}

func (s *MoRArenaSkl) init(size int) {
	s.segments = make([]*arenaskl.Skiplist, size)

	for i := 0; i < size; i++ {
		s.segments[i] = arenaskl.NewSkiplist(arenaskl.NewArena(MaxArenaSize))
	}
}

func (s *MoRArenaSkl) Name() string {
	return "mor_cow"
}

func (s *MoRArenaSkl) Put(key string, val []byte) {
	//1. Find curr segment
	currTs := time.Now()
	activeSegmentIdx := s.findSegmentIdx(currTs)

	internalKey := entry.KeyWithTs([]byte(key), timestamp.Now())

	var it arenaskl.Iterator
	it.Init(s.segments[activeSegmentIdx])

	err := it.Add(internalKey, val, 0)
	if err != nil {
		panic(err)
	}
}

func (s *MoRArenaSkl) Scan(startKey string, count int, snapshotTs time.Time) []entry.Pair[string, []byte] {
	//0. Check if snapshotTs has already expired
	if !timestamp.IsValidTs(snapshotTs, s.base.TTL) {
		return []entry.Pair[string, []byte]{}
	}

	//1. Get the Starting Segment
	segmentIdx := s.findSegmentIdx(snapshotTs)

	// 2.a Create Start Key
	internalKey := entry.KeyWithTs([]byte(startKey), timestamp.ToUnit64(snapshotTs))

	//2.b Init Heap
	mh := &MinHeap{}
	heap.Init(mh)

	// 2.c Fetch all iterators and add to PQ
	for i := 0; i < s.ttlValidSegmentsCount+1; i++ {
		pos := segmentIdx - i
		if pos < 0 {
			pos += len(s.segments)
		}

		//TODO: Fix
		var iter arenaskl.Iterator
		iter.Init(s.segments[pos])
		_ = iter.Seek(internalKey)
		if iter.Valid() {
			heap.Push(mh, iter)
		}
	}

	// 3.a Variable
	seenKeys := make(map[string]any)
	uniqueKVs := make(map[string][]byte)
	idx := 1
	snapshotTsNano := timestamp.ToUnit64(snapshotTs)

	for mh.Len() > 0 {
		smallestIter := heap.Pop(mh).(arenaskl.Iterator)
		itemKey := smallestIter.Key()
		itemVal := smallestIter.Value()
		if smallestIter.Next(); smallestIter.Valid() {
			heap.Push(mh, smallestIter)
		} else {
			smallestIter = arenaskl.Iterator{}
		}

		// 3.b scan logic
		if idx > count {
			break
		}
		// expiredTs < ItemTs < snapshotTs
		itemTs := entry.ParseTs(itemKey)
		lessThanOrEqualToSnapshotTs := itemTs <= snapshotTsNano
		greaterThanExpiredTs := timestamp.IsValidTsUint(itemTs, s.base.TTL)

		if lessThanOrEqualToSnapshotTs && greaterThanExpiredTs {
			strKey := string(entry.ParseKey(itemKey))
			if _, seen := seenKeys[strKey]; !seen {
				seenKeys[strKey] = true
				if itemVal != nil && len(itemVal) > 0 {
					uniqueKVs[strKey] = itemVal
					idx++
				}
			}
		}
	}

	for mh.Len() > 0 {
		heap.Pop(mh)
	}

	// 2. Range Scan delegation
	return entry.MapToArray(uniqueKVs)
}

func (s *MoRArenaSkl) Prune(_ uint64) int {

	currSegmentIdx := s.findSegmentIdx(time.Now())
	pruneSegmentIdx := currSegmentIdx - s.ttlValidSegmentsCount - 1
	if pruneSegmentIdx < 0 {
		pruneSegmentIdx += len(s.segments)
	}
	s.segments[pruneSegmentIdx] = arenaskl.NewSkiplist(arenaskl.NewArena(MaxArenaSize))

	return 0
}

func (s *MoRArenaSkl) Len() int {
	currTs := time.Now()
	activeSegmentIdx := s.findSegmentIdx(currTs)

	total := 0

	for i := 0; i < s.ttlValidSegmentsCount+1; i++ {
		pos := activeSegmentIdx - i
		if pos < 0 {
			pos += len(s.segments)
		}

		var it arenaskl.Iterator
		it.Init(s.segments[pos])
		for it.SeekToFirst(); it.Valid(); it.Next() {
			total++
		}
	}

	return total
}

func (s *MoRArenaSkl) Close() {
	for i, _ := range s.segments {
		s.segments[i] = nil
	}
}

func (s *MoRArenaSkl) findSegmentIdx(snapshotTs time.Time) int {
	cycleOffset := float64(snapshotTs.Unix() % s.cycleDuration)
	segmentIdx := int(math.Floor(cycleOffset / s.segmentDuration.Seconds()))
	return segmentIdx
}

func (s *MoRArenaSkl) Get(key string, snapshotTs time.Time) []byte {
	return s.base.Get(key, snapshotTs)
}

func (s *MoRArenaSkl) Delete(key string) {
	s.base.Delete(key)
}

func (s *MoRArenaSkl) StartGc(interval time.Duration, ctx context.Context) {
	s.base.StartGc(interval, ctx)
}
