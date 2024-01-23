package memtable

import (
	"context"
	common "github.com/arjunsk/cometkv/pkg/y_internal/entry"
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
