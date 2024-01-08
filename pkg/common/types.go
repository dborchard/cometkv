package common

import (
	"context"
	"time"
)

type IEphemeralMemtable interface {
	Put(key string, val []byte)
	Scan(startKey string, count int, snapshotTs time.Time) []Pair[string, []byte]
	Prune(expiredTs uint64) int

	Get(key string, snapshotTs time.Time) []byte
	Delete(key string)

	StartGc(interval time.Duration, ctx context.Context)
	Len() int
	Close()
}

type MemTableType int

const (
	SegmentRing MemTableType = iota
	VacuumSkipList
	VacuumBTree
	VacuumCoW
	MoRBTree
	MoRCoWBTree
	HWTBTree
	HWTCoWBTree
)
