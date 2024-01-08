package vacuum_skiplist

import (
	"cometkv/pkg/common"
	"cometkv/pkg/common/timestamp"
	"cometkv/pkg/impl/vacuum_skiplist/sl"
	"context"
	"time"
)

type EphemeralMemtable struct {
	base *common.EMBase
	list sl.SkipList[[]byte, []byte]
}

func New(gcInterval, ttl time.Duration, logStats bool, ctx context.Context) common.IEphemeralMemtable {

	tbl := EphemeralMemtable{}
	tbl.list = sl.New[[]byte, []byte](func(lhs, rhs []byte) int {
		return common.CompareKeys(lhs, rhs)
	}, sl.WithMutex())

	tbl.base = common.NewBase(&tbl, gcInterval, ttl, logStats)
	go tbl.StartGc(gcInterval, ctx)

	return &tbl
}

func (e *EphemeralMemtable) Put(key string, val []byte) {
	internalKey := common.KeyWithTs([]byte(key), timestamp.Now())
	e.list.Set(internalKey, val)
}

func (e *EphemeralMemtable) Scan(startKey string, count int, snapshotTs time.Time) []common.Pair[string, []byte] {
	//0. Check if snapshotTs has already expired
	if !timestamp.IsValidTs(snapshotTs, e.base.TTL) {
		return []common.Pair[string, []byte]{}
	}

	snapshotTsNano := timestamp.ToUnit64(snapshotTs)

	// 1. Do range scan 	{startKey, snapshotTs} <= item <= {endKey, snapshotTs}
	internalKey := common.KeyWithTs([]byte(startKey), snapshotTsNano)
	uniqueKVs := make(map[string][]byte)
	seenKeys := make(map[string]any)
	idx := 1

	e.list.Scan(internalKey, func(item *sl.Element[[]byte, []byte]) bool {
		if idx > count {
			return false
		}

		// expiredTs < ItemTs < snapshotTs
		itemTs := common.ParseTs(item.Key())
		lessThanOrEqualToSnapshotTs := itemTs <= snapshotTsNano
		greaterThanExpiredTs := timestamp.IsValidTsUint(itemTs, e.base.TTL)

		if lessThanOrEqualToSnapshotTs && greaterThanExpiredTs {
			strKey := string(common.ParseKey(item.Key()))
			if _, seen := seenKeys[strKey]; !seen {
				seenKeys[strKey] = true
				if item.Value != nil {
					uniqueKVs[strKey] = item.Value
					idx++
				}
			}
		}

		return true
	})

	// 2. Sorted key set
	return common.MapToArray(uniqueKVs)
}

func (e *EphemeralMemtable) Prune(expiredTs uint64) int {

	keysCopy := e.list.Keys()
	deleteCount := 0

	for _, key := range keysCopy {
		if common.ParseTs(key) <= expiredTs {
			e.list.Remove(key)
			deleteCount++
		}
	}

	return deleteCount
}

func (e *EphemeralMemtable) Len() int {
	return e.list.Len()
}

func (e *EphemeralMemtable) Close() {
	e.list.Init()
}

func (e *EphemeralMemtable) Get(key string, snapshotTs time.Time) []byte {
	return e.base.Get(key, snapshotTs)
}

func (e *EphemeralMemtable) Delete(key string) {
	e.base.Delete(key)
}

func (e *EphemeralMemtable) StartGc(interval time.Duration, ctx context.Context) {
	e.base.StartGc(interval, ctx)
}
