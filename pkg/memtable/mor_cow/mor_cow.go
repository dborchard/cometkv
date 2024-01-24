package mor_cow

import (
	"container/heap"
	"context"
	memtable "github.com/dborchard/cometkv/pkg/memtable"
	"github.com/dborchard/cometkv/pkg/memtable/base"
	"github.com/dborchard/cometkv/pkg/y/entry"
	"github.com/dborchard/cometkv/pkg/y/timestamp"
	"github.com/tidwall/btree"
	"math"
	"time"
)

// MoRCoW Ephemeral Copy-Ahead MV Tree
type MoRCoW struct {
	base                  *base.EMBase
	segments              []*BTreeGCoW[entry.Pair[[]byte, []byte]]
	ttlValidSegmentsCount int
	segmentDuration       time.Duration
	cycleDuration         int64
}

func New(gcInterval, ttl time.Duration, logStats bool, ctx context.Context) memtable.IMemtable {
	sr := MoRCoW{segmentDuration: gcInterval}
	sr.ttlValidSegmentsCount = int(math.Ceil(float64(ttl) / float64(sr.segmentDuration)))

	// init with segments
	totalSegments := sr.ttlValidSegmentsCount + 1 + 1
	sr.cycleDuration = int64(time.Duration(float64(totalSegments) * float64(gcInterval)).Seconds())
	sr.init(totalSegments)

	sr.base = base.NewBase(&sr, gcInterval, ttl, logStats)
	go sr.StartGc(gcInterval, ctx)

	return &sr
}

func (s *MoRCoW) init(size int) {
	s.segments = make([]*BTreeGCoW[entry.Pair[[]byte, []byte]], size)

	for i := 0; i < size; i++ {
		s.segments[i] = NewBTreeGCoW(func(a, b entry.Pair[[]byte, []byte]) bool {
			return entry.CompareKeys(a.Key, b.Key) < 0
		})
	}
}

func (s *MoRCoW) Name() string {
	return "mor_cow"
}

func (s *MoRCoW) Put(key string, val []byte) {
	//1. Find curr segment
	currTs := time.Now()
	activeSegmentIdx := s.findSegmentIdx(currTs)

	internalKey := entry.KeyWithTs([]byte(key), timestamp.Now())

	s.segments[activeSegmentIdx].Set(entry.Pair[[]byte, []byte]{
		Key: internalKey,
		Val: val,
	})
}

func (s *MoRCoW) Scan(startKey string, count int, opt memtable.ScanOptions) []entry.Pair[string, []byte] {
	snapshotTs := opt.SnapshotTs
	//0. Check if snapshotTs has already expired
	if !timestamp.IsValidTs(snapshotTs, s.base.TTL) {
		return []entry.Pair[string, []byte]{}
	}

	//1. Get the Starting Segment
	segmentIdx := s.findSegmentIdx(snapshotTs)

	// 2.a Create Start Key
	internalKey := entry.KeyWithTs([]byte(startKey), timestamp.ToUnit64(snapshotTs))
	startRow := entry.Pair[[]byte, []byte]{Key: internalKey}

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
		iter := s.segments[pos].Copy().Iter()
		iter.Seek(startRow)
		heap.Push(mh, &iter)
	}

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
		// expiredTs < ItemTs < snapshotTs
		itemTs := entry.ParseTs(item.Key)
		lessThanOrEqualToSnapshotTs := itemTs <= snapshotTsNano
		greaterThanExpiredTs := timestamp.IsValidTsUint(itemTs, s.base.TTL)

		if lessThanOrEqualToSnapshotTs && greaterThanExpiredTs {
			strKey := string(entry.ParseKey(item.Key))
			if _, seen := seenKeys[strKey]; !seen {
				seenKeys[strKey] = true
				if opt.IncludeFull || item.Val != nil {
					uniqueKVs[strKey] = item.Val
					idx++
				}
			}
		}
	}

	for mh.Len() > 0 {
		heap.Pop(mh).(*btree.IterG[entry.Pair[[]byte, []byte]]).Release()
	}

	// 2. Range Scan delegation
	return entry.MapToArray(uniqueKVs)
}

func (s *MoRCoW) Prune(_ uint64) int {

	currSegmentIdx := s.findSegmentIdx(time.Now())
	pruneSegmentIdx := currSegmentIdx - s.ttlValidSegmentsCount - 1
	if pruneSegmentIdx < 0 {
		pruneSegmentIdx += len(s.segments)
	}

	s.segments[pruneSegmentIdx].Clear()

	return 0
}

func (s *MoRCoW) Len() int {
	currTs := time.Now()
	activeSegmentIdx := s.findSegmentIdx(currTs)

	total := 0

	for i := 0; i < s.ttlValidSegmentsCount+1; i++ {
		pos := activeSegmentIdx - i
		if pos < 0 {
			pos += len(s.segments)
		}

		total += s.segments[pos].Len()
	}

	return total
}

func (s *MoRCoW) Close() {
	for _, segment := range s.segments {
		segment.Clear()
	}
}

func (s *MoRCoW) findSegmentIdx(snapshotTs time.Time) int {
	cycleOffset := float64(snapshotTs.Unix() % s.cycleDuration)
	segmentIdx := int(math.Floor(cycleOffset / s.segmentDuration.Seconds()))
	return segmentIdx
}

func (s *MoRCoW) Get(key string, snapshotTs time.Time) []byte {
	return s.base.Get(key, snapshotTs)
}

func (s *MoRCoW) Delete(key string) {
	s.base.Delete(key)
}

func (s *MoRCoW) StartGc(interval time.Duration, ctx context.Context) {
	s.base.StartGc(interval, ctx)
}
