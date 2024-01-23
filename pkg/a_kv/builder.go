package kv

import (
	"context"
	memtable "github.com/arjunsk/cometkv/pkg/b_memtable"
	"github.com/arjunsk/cometkv/pkg/b_memtable/hwt_btree"
	"github.com/arjunsk/cometkv/pkg/b_memtable/hwt_cow"
	"github.com/arjunsk/cometkv/pkg/b_memtable/mor_btree"
	"github.com/arjunsk/cometkv/pkg/b_memtable/mor_cow"
	"github.com/arjunsk/cometkv/pkg/b_memtable/segment_ring"
	"github.com/arjunsk/cometkv/pkg/b_memtable/vacuum_btree"
	"github.com/arjunsk/cometkv/pkg/b_memtable/vacuum_cow"
	"github.com/arjunsk/cometkv/pkg/b_memtable/vacuum_skiplist"
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
