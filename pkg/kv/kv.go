package kv

import (
	"context"
	"fmt"
	memtable "github.com/dborchard/cometkv/pkg/memtable"
	diskio "github.com/dborchard/cometkv/pkg/sst_storage"
	"github.com/dborchard/cometkv/pkg/y/entry"
	"sync/atomic"
	"time"
)

type KV interface {
	Put(key string, val []byte)
	Scan(startKey string, count int, snapshotTs time.Time) []entry.Pair[string, []byte]

	Get(key string, snapshotTs time.Time) []byte
	Delete(key string)
	Close()

	MemTableName() string
	SstStorageName() string
}

var _ KV = new(CometKV)

type CometKV struct {
	mem  memtable.IMemtable
	disk diskio.SstIO

	localInsertCounter          int64
	globalLongRangeScanCount    atomic.Int64
	globalLongRangeScanDuration time.Duration
}

func NewCometKV(ctx context.Context, mTyp memtable.Typ, dTyp diskio.Type, gcInterval, ttl, flushInterval time.Duration) KV {
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
	if c.disk != nil {
		//This is wrong.
		diff := count - len(res)
		if diff > 0 {
			res = append(res, c.disk.Scan(startKey, diff, snapshotTs)...)
		}
	}

	return res
}

func (c *CometKV) Get(key string, snapshotTs time.Time) []byte {
	res := c.mem.Get(key, snapshotTs)
	if c.disk != nil {
		if len(res) == 0 {
			res = c.disk.Get(key, snapshotTs)
		}
	}
	return res
}

func (c *CometKV) Delete(key string) {
	c.mem.Delete(key)
}

func (c *CometKV) Close() {
	c.mem.Close()
	if c.disk != nil {
		c.disk.Destroy()
	}
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
				if c.disk != nil {
					fmt.Printf("Flushing %d records to disk\n", len(records))
					_ = c.disk.Create(records)
				}

				endTs := time.Now()
				diff := endTs.Sub(startTs)

				c.globalLongRangeScanCount.Add(int64(len(records)))
				c.globalLongRangeScanDuration += diff
			}
		}
	}()
}

func (c *CometKV) atomicCasLocalInsertCounter() int64 {
	for {
		currentValue := atomic.LoadInt64(&c.localInsertCounter)
		if atomic.CompareAndSwapInt64(&c.localInsertCounter, currentValue, 0) {
			return currentValue
		} else {
			fmt.Println("CAS failed. Retrying...")
		}
	}
}

func (c *CometKV) MemTableName() string {
	return c.mem.Name()
}
func (c *CometKV) SstStorageName() string {
	if c.disk == nil {
		return "None"
	}
	return c.disk.Name()
}
