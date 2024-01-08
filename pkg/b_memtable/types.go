package memtable

import (
	"cometkv/pkg/b_memtable/hwt_btree"
	"cometkv/pkg/b_memtable/hwt_cow"
	"cometkv/pkg/b_memtable/mor_btree"
	"cometkv/pkg/b_memtable/mor_cow"
	"cometkv/pkg/b_memtable/segment_ring"
	"cometkv/pkg/b_memtable/vacuum_btree"
	"cometkv/pkg/b_memtable/vacuum_cow"
	"cometkv/pkg/b_memtable/vacuum_skiplist"
	common "cometkv/pkg/y_common"
	"context"
	"time"
)

type IMemtable interface {
	Put(key string, val []byte)
	Scan(startKey string, count int, snapshotTs time.Time) []common.Pair[string, []byte]
	Prune(expiredTs uint64) int

	Get(key string, snapshotTs time.Time) []byte
	Delete(key string)

	StartGc(interval time.Duration, ctx context.Context)
	Len() int
	Close()

	Name() string
}

type Type int

const (
	SegmentRing Type = iota
	VacuumSkipList
	VacuumBTree
	VacuumCoW
	MoRBTree
	MoRCoWBTree
	HWTBTree
	HWTCoWBTree
)

func New(typ Type, gcInterval, ttl time.Duration, logStats bool, ctx context.Context) (tree IMemtable) {

	switch typ {
	case SegmentRing:
		tree = segment_ring.New(gcInterval, ttl, logStats, ctx)

	case VacuumSkipList:
		tree = vacuum_skiplist.New(gcInterval, ttl, logStats, ctx)

	case VacuumBTree:
		tree = vacuum_btree.New(gcInterval, ttl, logStats, ctx)

	case VacuumCoW:
		tree = vacuum_cow.New(gcInterval, ttl, logStats, ctx)

	case MoRBTree:
		tree = mor_btree.New(gcInterval, ttl, logStats, ctx)

	case MoRCoWBTree:
		tree = mor_cow.New(gcInterval, ttl, logStats, ctx)

	case HWTBTree:
		tree = hwt_btree.New(gcInterval, ttl, logStats, ctx)

	case HWTCoWBTree:
		tree = hwt_cow.New(gcInterval, ttl, logStats, ctx)

	default:
		panic("unknown")
	}

	return
}
