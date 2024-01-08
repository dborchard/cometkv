package kv

import (
	"cometkv/pkg/b_memtable"
	diskio "cometkv/pkg/c_diskio"
	"cometkv/pkg/y_common"
	"context"
	"sync/atomic"
	"time"
)

type CometKV struct {
	memtable memtable.IMemtable
	disk     diskio.IDiskIO

	localInsertCounter          atomic.Int64
	globalLongRangeScanCount    atomic.Int64
	globalLongRangeScanDuration time.Duration
}

func NewCometKV(ctx context.Context, mTyp memtable.Type, dTyp diskio.Type) *CometKV {
	kv := CometKV{
		memtable: memtable.New(mTyp, 10, 10, true, ctx),
		disk:     diskio.New(dTyp),

		localInsertCounter:          atomic.Int64{},
		globalLongRangeScanCount:    atomic.Int64{},
		globalLongRangeScanDuration: time.Duration(0),
	}
	kv.startFlushThread(10*time.Second, true, ctx)
	return &kv
}
func (c *CometKV) Put(key string, val []byte) {
	c.memtable.Put(key, val)
	c.localInsertCounter.Add(1)
}

func (c *CometKV) Scan(startKey string, count int, snapshotTs time.Time) []common.Pair[string, []byte] {
	res := c.memtable.Scan(startKey, count, snapshotTs)
	if len(res) < count {
		res = append(res, c.disk.Scan(startKey, count-len(res), snapshotTs)...)
	}
	return res
}

func (c *CometKV) Get(key string, snapshotTs time.Time) []byte {
	res := c.memtable.Get(key, snapshotTs)
	if res == nil {
		res = c.disk.Get(key, snapshotTs)
	}
	return res
}

func (c *CometKV) Delete(key string) {
	c.memtable.Delete(key)
}

func (c *CometKV) Close() {
	c.memtable.Close()
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

					records := c.memtable.Scan("", int(totalInsertsForLongRangeDuration), time.Now())
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
