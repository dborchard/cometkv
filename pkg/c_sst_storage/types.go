package sstio

import (
	"github.com/dborchard/cometkv/pkg/c_sst_storage/mem_btree"
	common "github.com/dborchard/cometkv/pkg/y_internal/entry"
	"time"
)

type SstIO interface {
	Scan(startKey string, count int, snapshotTs time.Time) []common.Pair[string, []byte]
	Get(key string, snapshotTs time.Time) []byte
	Create(records []common.Pair[string, []byte]) error
	Destroy()

	Name() string
}

var _ SstIO = new(mem_btree.IO)

type Type int

const (
	MBtree Type = iota
)

func New(t Type) SstIO {
	switch t {
	case MBtree:
		return mem_btree.NewMBtreeIO()
	default:
		panic("unknown disk_io type")
	}
}
