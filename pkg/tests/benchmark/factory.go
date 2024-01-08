package main

import (
	"cometkv/pkg/common"
	"cometkv/pkg/impl/hwt_btree"
	"cometkv/pkg/impl/hwt_cow"
	"cometkv/pkg/impl/mor_btree"
	"cometkv/pkg/impl/mor_cow"
	"cometkv/pkg/impl/segment_ring"
	"cometkv/pkg/impl/vacuum_btree"
	"cometkv/pkg/impl/vacuum_cow"
	"cometkv/pkg/impl/vacuum_skiplist"
	"context"
	"time"
)

func Build(gcInterval time.Duration, ttl time.Duration, typ common.MemTableType, ctx context.Context, logStats bool) (tree common.IEphemeralMemtable, name string) {
	switch typ {
	case common.SegmentRing:
		tree = segment_ring.New(gcInterval, ttl, logStats, ctx)
		name = "seg_ring"

	case common.VacuumSkipList:
		tree = vacuum_skiplist.New(gcInterval, ttl, logStats, ctx)
		name = "vacuum_sl"
	case common.VacuumBTree:
		tree = vacuum_btree.New(gcInterval, ttl, logStats, ctx)
		name = "vacuum_btree"
	case common.VacuumCoW:
		tree = vacuum_cow.New(gcInterval, ttl, logStats, ctx)
		name = "vacuum_cow"

	case common.MoRBTree:
		tree = mor_btree.New(gcInterval, ttl, logStats, ctx)
		name = "mor_btree"
	case common.MoRCoWBTree:
		tree = mor_cow.New(gcInterval, ttl, logStats, ctx)
		name = "mor_cow_btree"

	case common.HWTBTree:
		tree = hwt_btree.New(gcInterval, ttl, logStats, ctx)
		name = "hwt_btree"
	case common.HWTCoWBTree:
		tree = hwt_cow.New(gcInterval, ttl, logStats, ctx)
		name = "hwt_cow_btree"

	default:
		panic("unknown")
	}
	return tree, name
}
