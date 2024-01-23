package vacuum_skiplist

import (
	"context"
	"github.com/arjunsk/cometkv/pkg/b_memtable/base"
	"github.com/arjunsk/cometkv/pkg/b_memtable/vacuum_skiplist/sl"
	"github.com/arjunsk/cometkv/pkg/y_internal/entry"
	"github.com/arjunsk/cometkv/pkg/y_internal/timestamp"
	"time"
)

type EphemeralMemtable struct {
	base *base.EMBase
	list sl.SkipList[[]byte, []byte]
}

func New(gcInterval, ttl time.Duration, logStats bool, ctx context.Context) memtable.IMemtable {

	tbl := EphemeralMemtable{}
	tbl.list = sl.New[[]byte, []byte](func(lhs, rhs []byte) int {
		return entry.CompareKeys(lhs, rhs)
	}, sl.WithMutex())

	tbl.base = base.NewBase(&tbl, gcInterval, ttl, logStats)
	go tbl.StartGc(gcInterval, ctx)

	return &tbl
}

func (e *EphemeralMemtable) Name() string {
	return "vacuum_sl"
}

func (e *EphemeralMemtable) Put(key string, val []byte) {
	internalKey := entry.KeyWithTs([]byte(key), timestamp.Now())
	e.list.Set(internalKey, val)
}

func (e *EphemeralMemtable) Scan(startKey string, count int, snapshotTs time.Time) []entry.Pair[string, []byte] {
	//0. Check if snapshotTs has already expired
	if !timestamp.IsValidTs(snapshotTs, e.base.TTL) {
		return []entry.Pair[string, []byte]{}
	}

	snapshotTsNano := timestamp.ToUnit64(snapshotTs)

	// 1. Do range scan 	{startKey, snapshotTs} <= item <= {endKey, snapshotTs}
	internalKey := entry.KeyWithTs([]byte(startKey), snapshotTsNano)
	uniqueKVs := make(map[string][]byte)
	seenKeys := make(map[string]any)
	idx := 1

	e.list.Scan(internalKey, func(item *sl.Element[[]byte, []byte]) bool {
		if idx > count {
			return false
		}

		// expiredTs < ItemTs < snapshotTs
		itemTs := entry.ParseTs(item.Key())
		lessThanOrEqualToSnapshotTs := itemTs <= snapshotTsNano
		greaterThanExpiredTs := timestamp.IsValidTsUint(itemTs, e.base.TTL)

		if lessThanOrEqualToSnapshotTs && greaterThanExpiredTs {
			strKey := string(entry.ParseKey(item.Key()))
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
	return entry.MapToArray(uniqueKVs)
}

func (e *EphemeralMemtable) Prune(expiredTs uint64) int {

	keysCopy := e.list.Keys()
	deleteCount := 0

	for _, key := range keysCopy {
		if entry.ParseTs(key) <= expiredTs {
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
