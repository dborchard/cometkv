package kv

import (
	"context"
	memtable "github.com/dborchard/cometkv/pkg/memtable"
	"github.com/dborchard/cometkv/pkg/memtable/hwt_btree"
	"github.com/dborchard/cometkv/pkg/memtable/hwt_cow"
	"github.com/dborchard/cometkv/pkg/memtable/mor_arenaskl"
	"github.com/dborchard/cometkv/pkg/memtable/mor_btree"
	"github.com/dborchard/cometkv/pkg/memtable/mor_cow"
	"github.com/dborchard/cometkv/pkg/memtable/segment_ring"
	"github.com/dborchard/cometkv/pkg/memtable/segment_ring_arenaskl"
	"github.com/dborchard/cometkv/pkg/memtable/vacuum_btree"
	"github.com/dborchard/cometkv/pkg/memtable/vacuum_cow"
	"github.com/dborchard/cometkv/pkg/memtable/vacuum_skiplist"
	"time"
)

func NewMemtable(typ memtable.Typ, gcInterval, ttl time.Duration, logStats bool, ctx context.Context) (tree memtable.IMemtable) {

	switch typ {
	case memtable.SegmentRing:
		tree = segment_ring.New(gcInterval, ttl, logStats, ctx)

	case memtable.SegmentArenaSkl:
		tree = segment_ring_arenaskl.New(gcInterval, ttl, logStats, ctx)

	case memtable.VacuumSkipList:
		tree = vacuum_skiplist.New(gcInterval, ttl, logStats, ctx)

	case memtable.VacuumBTree:
		tree = vacuum_btree.New(gcInterval, ttl, logStats, ctx)

	case memtable.VacuumCoW:
		tree = vacuum_cow.New(gcInterval, ttl, logStats, ctx)

	case memtable.MoRBTree:
		tree = mor_btree.New(gcInterval, ttl, logStats, ctx)

	case memtable.MoRCoWBTree:
		tree = mor_cow.New(gcInterval, ttl, logStats, ctx)

	case memtable.MoRArenaSkl:
		tree = mor_arenaskl.New(gcInterval, ttl, logStats, ctx)

	case memtable.HWTBTree:
		tree = hwt_btree.New(gcInterval, ttl, logStats, ctx)

	case memtable.HWTCoWBTree:
		tree = hwt_cow.New(gcInterval, ttl, logStats, ctx)

	default:
		panic("unknown")
	}

	return
}
