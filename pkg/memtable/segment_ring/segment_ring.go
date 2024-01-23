package segment_ring

import (
	"container/list"
	"context"
	memtable "github.com/dborchard/cometkv/pkg/memtable"
	"github.com/dborchard/cometkv/pkg/memtable/base"
	"github.com/dborchard/cometkv/pkg/y/entry"
	"github.com/dborchard/cometkv/pkg/y/timestamp"
	"math"
	"time"
)

// SegmentRing Ephemeral Copy-Ahead MV Tree
type SegmentRing struct {
	base *base.EMBase

	segments []*Segment

	ttlValidSegmentsCount int

	segmentDuration time.Duration
	cycleDuration   int64
}

func New(gcInterval, ttl time.Duration, logStats bool, ctx context.Context) memtable.IMemtable {

	// create SegmentRing instance
	sr := SegmentRing{segmentDuration: gcInterval}

	// calc copy-ahead segment count
	sr.ttlValidSegmentsCount = int(math.Ceil(float64(ttl) / float64(sr.segmentDuration)))

	// init with segments
	totalSegments := 3*sr.ttlValidSegmentsCount + 2
	sr.cycleDuration = int64(time.Duration(float64(totalSegments) * float64(gcInterval)).Seconds())
	sr.init(totalSegments, ctx)

	sr.base = base.NewBase(&sr, gcInterval, ttl, logStats)
	go sr.StartGc(gcInterval, ctx)

	return &sr
}

func (s *SegmentRing) init(size int, ctx context.Context) {
	s.segments = make([]*Segment, size)

	// Create segment array
	for i := 0; i < size; i++ {
		s.segments[i] = NewSegment(ctx)
	}

	// Create Ptr ring
	for i := 0; i < size-1; i++ {
		s.segments[i].nextPtr = s.segments[i+1]
	}
	s.segments[size-1].nextPtr = s.segments[0]

	// Start Listeners in all the Segments
	for i := 0; i < size; i++ {
		s.segments[i].StartListener()
	}
}

func (s *SegmentRing) Name() string {
	return "segment_ring"
}

func (s *SegmentRing) Put(key string, val []byte) {
	//1. Find curr segment
	currTs := time.Now()
	activeSegmentIdx := s.findSegmentIdx(currTs)

	// 2. Add to Segment "VLOG"
	rPtr := s.segments[activeSegmentIdx].AddValue(val)

	// 3. Create entry for "Index"
	internalKey := entry.KeyWithTs([]byte(key), timestamp.ToUnit64(currTs))
	entry := &entry.Pair[[]byte, *list.Element]{Key: internalKey, Val: rPtr}

	// 4.a Add to Curr segment in sync.
	head := s.segments[activeSegmentIdx]
	head.AddIndex(entry)
	head = head.nextPtr

	// 4.b Parallel write to Curr+1 .... segments async.
	// Due to wait group in segment, we are guaranteed that Scans will see the written value.
	for i := 1; i <= s.ttlValidSegmentsCount; i++ {
		head.AddIndexAsync(entry)
		head = head.nextPtr
	}
}

func (s *SegmentRing) Scan(startKey string, count int, snapshotTs time.Time) []entry.Pair[string, []byte] {
	//0. Check if snapshotTs has already expired
	if !timestamp.IsValidTs(snapshotTs, s.base.TTL) {
		return []entry.Pair[string, []byte]{}
	}

	//1. Get Segment
	segmentIdx := s.findSegmentIdx(snapshotTs)

	// 2. Range Scan delegation
	return s.segments[segmentIdx].Scan(startKey, count, snapshotTs)
}

func (s *SegmentRing) Prune(_ uint64) int {

	currSegmentIdx := s.findSegmentIdx(time.Now())
	pruneSegmentIdx := currSegmentIdx - 1 - (2 * s.ttlValidSegmentsCount)
	if pruneSegmentIdx < 0 {
		pruneSegmentIdx += len(s.segments)
	}

	deleteCount := s.segments[pruneSegmentIdx].Free()

	return deleteCount
}

func (s *SegmentRing) Len() int {
	currTs := time.Now()
	activeSegmentIdx := s.findSegmentIdx(currTs)

	return s.segments[activeSegmentIdx].Len()
}

func (s *SegmentRing) Close() {
	for _, segment := range s.segments {
		segment.Free()
	}
}

func (s *SegmentRing) findSegmentIdx(snapshotTs time.Time) int {
	cycleOffset := float64(snapshotTs.Unix() % s.cycleDuration)
	segmentIdx := int(math.Floor(cycleOffset / s.segmentDuration.Seconds()))
	return segmentIdx
}

func (s *SegmentRing) Get(key string, snapshotTs time.Time) []byte {
	return s.base.Get(key, snapshotTs)
}

func (s *SegmentRing) Delete(key string) {
	s.base.Delete(key)
}

func (s *SegmentRing) StartGc(interval time.Duration, ctx context.Context) {
	s.base.StartGc(interval, ctx)
}
