package hwt_btree

import (
	"cometkv/pkg/b_memtable"
	"cometkv/pkg/y_common"
	"cometkv/pkg/y_common/timestamp"
	"context"
	"github.com/RussellLuo/timingwheel"
	"github.com/tidwall/btree"
	"time"
)

type EphemeralMemtable struct {
	base *memtable.EMBase

	timer *timingwheel.TimingWheel
	tree  *btree.BTreeG[common.Pair[[]byte, []byte]]
}

func (e *EphemeralMemtable) Name() string {
	return "hwt_btree"
}

func New(gcInterval, ttl time.Duration, logStats bool, ctx context.Context) memtable.IMemtable {

	bt := EphemeralMemtable{}

	bt.tree = btree.NewBTreeG(func(a, b common.Pair[[]byte, []byte]) bool {
		return common.CompareKeys(a.Key, b.Key) < 0
	})

	bt.timer = timingwheel.NewTimingWheel(time.Second, int64(ttl.Seconds()))
	bt.base = memtable.NewBase(&bt, gcInterval, ttl, logStats)
	go bt.StartGc(gcInterval, ctx)
	go bt.timer.Start()

	return &bt
}

func (e *EphemeralMemtable) Put(key string, val []byte) {
	internalKey := common.KeyWithTs([]byte(key), timestamp.Now())

	row := common.Pair[[]byte, []byte]{
		Key: internalKey,
		Val: val,
	}
	e.tree.Set(row)

	e.timer.AfterFunc(e.base.TTL, func() {
		e.tree.Delete(row)
	})
}

func (e *EphemeralMemtable) Scan(startKey string, count int, snapshotTs time.Time) []common.Pair[string, []byte] {
	//0. Check if snapshotTs has already expired
	if !timestamp.IsValidTs(snapshotTs, e.base.TTL) {
		return []common.Pair[string, []byte]{}
	}

	snapshotTsNano := timestamp.ToUnit64(snapshotTs)

	// 1. Do range scan
	internalKey := common.KeyWithTs([]byte(startKey), timestamp.ToUnit64(snapshotTs))
	startRow := common.Pair[[]byte, []byte]{Key: internalKey}
	seenKeys := make(map[string]any)
	uniqueKVs := make(map[string][]byte)
	idx := 1
	e.tree.Ascend(startRow, func(item common.Pair[[]byte, []byte]) bool {

		if idx > count {
			return false
		}

		// expiredTs < ItemTs < snapshotTs
		itemTs := common.ParseTs(item.Key)
		lessThanOrEqualToSnapshotTs := itemTs <= snapshotTsNano
		greaterThanExpiredTs := timestamp.IsValidTsUint(itemTs, e.base.TTL)

		if lessThanOrEqualToSnapshotTs && greaterThanExpiredTs {
			strKey := string(common.ParseKey(item.Key))
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

	return common.MapToArray(uniqueKVs)
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
