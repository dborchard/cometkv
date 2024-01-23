package kv

import (
	"github.com/arjunsk/cometkv/pkg/y_internal/entry"
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
