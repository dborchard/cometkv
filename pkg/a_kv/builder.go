package kv

import (
	memtable "cometkv/pkg/b_memtable"
	"cometkv/pkg/b_memtable/hwt_btree"
	"cometkv/pkg/b_memtable/hwt_cow"
	"cometkv/pkg/b_memtable/mor_btree"
	"cometkv/pkg/b_memtable/mor_cow"
	"cometkv/pkg/b_memtable/segment_ring"
	"cometkv/pkg/b_memtable/vacuum_btree"
	"cometkv/pkg/b_memtable/vacuum_cow"
	"cometkv/pkg/b_memtable/vacuum_skiplist"
	"context"
	"time"
)

func NewMemtable(typ MemtableTyp, gcInterval, ttl time.Duration, logStats bool, ctx context.Context) (tree memtable.IMemtable) {

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
