package hwt_btree

import (
	"context"
	"github.com/RussellLuo/timingwheel"
	memtable "github.com/dborchard/cometkv/pkg/memtable"
	"github.com/dborchard/cometkv/pkg/memtable/base"
	"github.com/dborchard/cometkv/pkg/y/entry"
	"github.com/dborchard/cometkv/pkg/y/timestamp"
	"github.com/tidwall/btree"
	"time"
)

type EphemeralMemtable struct {
	base *base.EMBase

	timer *timingwheel.TimingWheel
	tree  *btree.BTreeG[entry.Pair[[]byte, []byte]]
}

func (e *EphemeralMemtable) Name() string {
	return "hwt_btree"
}

func New(gcInterval, ttl time.Duration, logStats bool, ctx context.Context) memtable.IMemtable {

	bt := EphemeralMemtable{}

	bt.tree = btree.NewBTreeG(func(a, b entry.Pair[[]byte, []byte]) bool {
		return entry.CompareKeys(a.Key, b.Key) < 0
	})

	bt.timer = timingwheel.NewTimingWheel(time.Second, int64(ttl.Seconds()))
	bt.base = base.NewBase(&bt, gcInterval, ttl, logStats)
	go bt.StartGc(gcInterval, ctx)
	go bt.timer.Start()

	return &bt
}

func (e *EphemeralMemtable) Put(key string, val []byte) {
	internalKey := entry.KeyWithTs([]byte(key), timestamp.Now())

	row := entry.Pair[[]byte, []byte]{
		Key: internalKey,
		Val: val,
	}
	e.tree.Set(row)

	e.timer.AfterFunc(e.base.TTL, func() {
		e.tree.Delete(row)
	})
}

func (e *EphemeralMemtable) Scan(startKey string, count int, snapshotTs time.Time) []entry.Pair[string, []byte] {
	//0. Check if snapshotTs has already expired
	if !timestamp.IsValidTs(snapshotTs, e.base.TTL) {
		return []entry.Pair[string, []byte]{}
	}

	snapshotTsNano := timestamp.ToUnit64(snapshotTs)

	// 1. Do range scan
	internalKey := entry.KeyWithTs([]byte(startKey), timestamp.ToUnit64(snapshotTs))
	startRow := entry.Pair[[]byte, []byte]{Key: internalKey}
	seenKeys := make(map[string]any)
	uniqueKVs := make(map[string][]byte)
	idx := 1
	e.tree.Ascend(startRow, func(item entry.Pair[[]byte, []byte]) bool {

		if idx > count {
			return false
		}

		// expiredTs < ItemTs < snapshotTs
		itemTs := entry.ParseTs(item.Key)
		lessThanOrEqualToSnapshotTs := itemTs <= snapshotTsNano
		greaterThanExpiredTs := timestamp.IsValidTsUint(itemTs, e.base.TTL)

		if lessThanOrEqualToSnapshotTs && greaterThanExpiredTs {
			strKey := string(entry.ParseKey(item.Key))
			if _, seen := seenKeys[strKey]; !seen {
				seenKeys[strKey] = true
				if item.Val != nil {
					uniqueKVs[strKey] = item.Val
					idx++
				}
			}
		}

		return true
	})

	return entry.MapToArray(uniqueKVs)
}

func (e *EphemeralMemtable) Prune(expiredTs uint64) int {
	return 0
}

func (e *EphemeralMemtable) Len() int {
	return e.tree.Len()
}

func (e *EphemeralMemtable) Close() {
	e.tree.Clear()
	e.timer.Stop()
}

func (e *EphemeralMemtable) StartGc(interval time.Duration, ctx context.Context) {
	e.base.StartGc(interval, ctx)
}

func (e *EphemeralMemtable) Get(key string, snapshotTs time.Time) []byte {
	return e.base.Get(key, snapshotTs)
}

func (e *EphemeralMemtable) Delete(key string) {
	e.base.Delete(key)
}
