package base

import (
	"context"
	"fmt"
	movingaverage "github.com/RobinUS2/golang-moving-average"
	memtable "github.com/dborchard/cometkv/pkg/memtable"
	"github.com/dborchard/cometkv/pkg/y/entry"
	"github.com/dborchard/cometkv/pkg/y/timestamp"
	"runtime"
	"time"
)

type EMBase struct {
	TTL      time.Duration
	derived  memtable.IMemtable
	moAvg    *movingaverage.MovingAverage
	logStats bool
}

func NewBase(bt memtable.IMemtable, gc, ttl time.Duration, logStats bool) *EMBase {
	return &EMBase{
		derived:  bt,
		TTL:      ttl,
		moAvg:    movingaverage.New(int(ttl / gc)), // moving average of last 1min
		logStats: logStats,
	}
}

func (e *EMBase) Get(key string, snapshotTs time.Time) []byte {
	res := e.derived.Scan(key, 1, snapshotTs)
	if len(res) != 1 || res[0].Key != key {
		return []byte{}
	}
	return res[0].Val
}

func (e *EMBase) Delete(key string) {
	e.derived.Put(key, nil)
}

func (e *EMBase) StartGc(interval time.Duration, ctx context.Context) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			expiredTs := time.Now().Add(-1 * e.TTL)
			e.Prune(timestamp.ToUnit64(expiredTs))
		case <-ctx.Done():
			return
		}
	}
}

func (e *EMBase) Prune(expiredTs uint64) int {
	startTs := time.Now()

	// ref call.
	delCount := e.derived.Prune(expiredTs)

	endTs := time.Now()
	diff := endTs.Sub(startTs)

	e.moAvg.Add(float64(diff.Nanoseconds()))
	avgDiff := time.Duration(e.moAvg.Avg())

	runtime.GC()

	if e.logStats {
		fmt.Printf("Deleted %d elements in %s. Avg pruning time %s \n", delCount, diff, avgDiff)
	}
	return delCount
}
func (e *EMBase) Put(key string, val []byte) { panic("not implemented") }
func (e *EMBase) Scan(k string, c int, Ts time.Time) []entry.Pair[string, []byte] {
	panic("not implemented")
}

func (e *EMBase) Name() string {
	return e.derived.Name()
}

func (e *EMBase) Len() int { panic("not implemented") }
func (e *EMBase) Close()   { panic("not implemented") }
