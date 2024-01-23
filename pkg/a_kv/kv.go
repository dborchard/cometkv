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

	localInsertCounter          atomic.Int64
	globalLongRangeScanCount    atomic.Int64
	globalLongRangeScanDuration time.Duration
}

func NewCometKV(ctx context.Context, mTyp MemtableTyp, dTyp diskio.Type, ttl, longRangeDuration time.Duration) KV {
	kv := CometKV{
		mem:  NewMemtable(mTyp, longRangeDuration, ttl, true, ctx),
		disk: diskio.New(dTyp),

		localInsertCounter:          atomic.Int64{},
		globalLongRangeScanCount:    atomic.Int64{},
		globalLongRangeScanDuration: time.Duration(0),
	}
	kv.startFlushThread(10*time.Second, true, ctx)
	return &kv
}
func (c *CometKV) Put(key string, val []byte) {
	c.mem.Put(key, val)
	c.localInsertCounter.Add(1)
}

func (c *CometKV) Scan(startKey string, count int, snapshotTs time.Time) []entry.Pair[string, []byte] {
	res := c.mem.Scan(startKey, count, snapshotTs)
	if len(res) < count {
		res = append(res, c.disk.Scan(startKey, count-len(res), snapshotTs)...)
	}
	return res
}

func (c *CometKV) Get(key string, snapshotTs time.Time) []byte {
	res := c.mem.Get(key, snapshotTs)
	if res == nil {
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

func (c *CometKV) startFlushThread(longRangeDuration time.Duration, startLongRangeScan bool, ctx context.Context) {
	go func() {
		ticker := time.NewTicker(longRangeDuration)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if startLongRangeScan {
					totalInsertsForLongRangeDuration := c.localInsertCounter.Load()
					c.localInsertCounter.Store(0)

					startTs := time.Now()

					records := c.mem.Scan("", int(totalInsertsForLongRangeDuration), time.Now())
					_ = c.disk.Create(records)

					endTs := time.Now()
					diff := endTs.Sub(startTs)

					c.globalLongRangeScanCount.Add(int64(len(records)))
					c.globalLongRangeScanDuration += diff
				}
			}
		}
	}()
}

func (c *CometKV) Name() string {
	return c.mem.Name()
}
