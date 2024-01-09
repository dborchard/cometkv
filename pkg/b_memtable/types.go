package memtable

import (
	common "cometkv/pkg/y_common"
	"context"
	"time"
)

type IMemtable interface {
	Put(key string, val []byte)
	Scan(startKey string, count int, snapshotTs time.Time) []common.Pair[string, []byte]
	Prune(expiredTs uint64) int

	Get(key string, snapshotTs time.Time) []byte
	Delete(key string)

	StartGc(interval time.Duration, ctx context.Context)
	Len() int
	Close()

	Name() string
}
