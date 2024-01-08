package vacuum_btree

import (
	"cometkv/pkg/common"
	"cometkv/pkg/common/timestamp"
	"context"
	"github.com/tidwall/btree"
	"time"
)

type EphemeralMemtable struct {
	base *common.EMBase

	tree *btree.BTreeG[common.Pair[[]byte, []byte]]
}

func New(gcInterval, ttl time.Duration, logStats bool, ctx context.Context) common.IEphemeralMemtable {

	bt := EphemeralMemtable{}

	bt.tree = btree.NewBTreeG(func(a, b common.Pair[[]byte, []byte]) bool {
		return common.CompareKeys(a.Key, b.Key) < 0
	})

	bt.base = common.NewBase(&bt, gcInterval, ttl, logStats)
	go bt.StartGc(gcInterval, ctx)

	return &bt
}

func (e *EphemeralMemtable) Put(key string, val []byte) {
	internalKey := common.KeyWithTs([]byte(key), timestamp.Now())

	e.tree.Set(common.Pair[[]byte, []byte]{
		Key: internalKey,
		Val: val,
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

	var rowsCopy []common.Pair[[]byte, []byte]
	e.tree.Scan(func(item common.Pair[[]byte, []byte]) bool {
		rowsCopy = append(rowsCopy, item)
		return true
	})

	deleteCount := 0
	for _, row := range rowsCopy {
		if common.ParseTs(row.Key) <= expiredTs {
			e.tree.Delete(row)
			deleteCount++
		}
	}

	return deleteCount
}

func (e *EphemeralMemtable) Len() int {
	return e.tree.Len()
}

func (e *EphemeralMemtable) Close() {
	e.tree.Clear()
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
