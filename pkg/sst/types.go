package sst

import (
	"github.com/dborchard/cometkv/pkg/sst/mem_btree"
	common "github.com/dborchard/cometkv/pkg/y/entry"
	"time"
)

type IO interface {
	Scan(startKey string, count int, snapshotTs time.Time) []common.Pair[string, []byte]
	Get(key string, snapshotTs time.Time) []byte
	Create(records []common.Pair[string, []byte]) error
	Destroy()

	Name() string
}

var _ IO = new(mem_btree.IO)

type Type int

const (
	MBtree Type = iota
)

func NewSstIO(t Type) IO {
	switch t {
	case MBtree:
		return mem_btree.NewMBtreeIO()
	default:
		panic("unknown disk_io type")
	}
}
