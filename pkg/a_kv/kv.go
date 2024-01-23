package kv

import (
	"context"
	memtable "github.com/arjunsk/cometkv/pkg/b_memtable"
	diskio "github.com/arjunsk/cometkv/pkg/c_sst_storage"
	"github.com/arjunsk/cometkv/pkg/y_internal/entry"
	"sync/atomic"
	"time"
)

type CometKV struct {
	mem  memtable.IMemtable
	disk diskio.SstIO

	localInsertCounter          int64
	globalLongRangeScanCount    atomic.Int64
	globalLongRangeScanDuration time.Duration
}

func NewCometKV(ctx context.Context, mTyp memtable.MemtableTyp, dTyp diskio.Type, gcInterval, ttl, flushInterval time.Duration) KV {
	kv := CometKV{
		mem:  NewMemtable(mTyp, gcInterval, ttl, true, ctx),
		disk: diskio.New(dTyp),

		localInsertCounter:          0,
		globalLongRangeScanCount:    atomic.Int64{},
		globalLongRangeScanDuration: time.Duration(0),
	}
	kv.startFlushThread(flushInterval, ctx)
	return &kv
}
func (c *CometKV) Put(key string, val []byte) {
	c.mem.Put(key, val)
	c.localInsertCounter++
}

func (c *CometKV) Scan(startKey string, count int, snapshotTs time.Time) []entry.Pair[string, []byte] {
	res := c.mem.Scan(startKey, count, snapshotTs)
	diff := count - len(res)
	if diff > 0 {
		res = append(res, c.disk.Scan(startKey, diff, snapshotTs)...)
	}
	return res
}

func (c *CometKV) Get(key string, snapshotTs time.Time) []byte {
	res := c.mem.Get(key, snapshotTs)
	if len(res) == 0 {
		res = c.disk.Get(key, snapshotTs)
	}
	return res
}

func (c *CometKV) Delete(key string) {
	c.mem.Delete(key)
}

func (c *CometKV) Close() {
	c.mem.Close()
	c.disk.Destroy()
}

func (c *CometKV) startFlushThread(flushInterval time.Duration, ctx context.Context) {
	go func() {
		ticker := time.NewTicker(flushInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				totalInsertsForLongRangeDuration := c.atomicCasLocalInsertCounter()

				startTs := time.Now()

				records := c.mem.Scan("", int(totalInsertsForLongRangeDuration), time.Now())
				_ = c.disk.Create(records)

				endTs := time.Now()
				diff := endTs.Sub(startTs)

				c.globalLongRangeScanCount.Add(int64(len(records)))
				c.globalLongRangeScanDuration += diff
			}
		}
	}()
}

func (c *CometKV) MemTableName() string {
	return c.mem.Name()
}

func (c *CometKV) atomicCasLocalInsertCounter() int64 {
	for {
		currentValue := atomic.LoadInt64(&c.localInsertCounter)
		if atomic.CompareAndSwapInt64(&c.localInsertCounter, currentValue, 0) {
			return currentValue
		}
	}
}
