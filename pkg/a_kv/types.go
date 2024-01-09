package kv

import (
	"cometkv/pkg/y_common"
	"time"
)

type KV interface {
	Put(key string, val []byte)
	Scan(startKey string, count int, snapshotTs time.Time) []common.Pair[string, []byte]

	Get(key string, snapshotTs time.Time) []byte
	Delete(key string)
	Close()
}

var _ KV = new(CometKV)

type MemtableTyp int

const (
	SegmentRing MemtableTyp = iota
	VacuumSkipList
	VacuumBTree
	VacuumCoW
	MoRBTree
	MoRCoWBTree
	HWTBTree
	HWTCoWBTree
)
