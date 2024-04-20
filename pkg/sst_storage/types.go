package sstio

import (
	"github.com/dborchard/cometkv/pkg/sst_storage/mem_btree"
	common "github.com/dborchard/cometkv/pkg/y/entry"
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
	Nil
)

func New(t Type) SstIO {
	switch t {
	case MBtree:
		return mem_btree.NewMBtreeIO()
	default:
		return nil
	}
}
