package memtable

import (
	"context"
	common "github.com/dborchard/cometkv/pkg/y/entry"
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

type Typ int

const (
	SegmentRing Typ = iota
	VacuumSkipList
	VacuumBTree
	VacuumCoW
	MoRBTree
	MoRCoWBTree
	HWTBTree
	HWTCoWBTree
)
