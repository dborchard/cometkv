package kv

import (
	"context"
	"fmt"
	"github.com/dborchard/cometkv/pkg/memtable"
	"github.com/dborchard/cometkv/pkg/sst"
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
	mem                memtable.IMemtable
	sst                sst.IO
	localInsertCounter int64
}

func NewCometKV(ctx context.Context, mTyp memtable.Typ, dTyp sst.Type, gcInterval, ttl, flushInterval time.Duration) KV {
	kv := CometKV{
		mem:                NewMemtable(mTyp, gcInterval, ttl, true, ctx),
		sst:                sst.NewSstIO(dTyp),
		localInsertCounter: 0,
	}
	kv.startFlushThread(flushInterval, ctx)
	return &kv
}
func (c *CometKV) Put(key string, val []byte) {
	c.mem.Put(key, val)
	c.localInsertCounter++
}

func (c *CometKV) Scan(startKey string, count int, snapshotTs time.Time) []entry.Pair[string, []byte] {
	res := c.mem.Scan(startKey, count, memtable.ScanOptions{SnapshotTs: snapshotTs})
	diff := count - len(res)
	if diff > 0 {
		res = append(res, c.sst.Scan(startKey, diff, snapshotTs)...)
	}
	return res
}

func (c *CometKV) Get(key string, snapshotTs time.Time) []byte {
	res := c.mem.Get(key, snapshotTs)
	if res == nil {
		// means key is deleted
		return nil
	}
	if len(res) == 0 {
		// means key not found in memtable. Try sst.
		res = c.sst.Get(key, snapshotTs)
	}
	return res
}

func (c *CometKV) Delete(key string) {
	c.mem.Delete(key)
}

func (c *CometKV) Close() {
	c.mem.Close()
	c.sst.Destroy()
	c.localInsertCounter = 0
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
				records := c.mem.Scan("", int(totalInsertsForLongRangeDuration), memtable.ScanOptions{SnapshotTs: time.Now()})
				_ = c.sst.Create(records)
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
	return c.sst.Name()
}
