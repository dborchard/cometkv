package diskio

import (
	"cometkv/pkg/c_diskio/mbtree"
	common "cometkv/pkg/y_common"
	"time"
)

type IDiskIO interface {
	Scan(startKey string, count int, snapshotTs time.Time) []common.Pair[string, []byte]
	Get(key string, snapshotTs time.Time) []byte
	Create(records []common.Pair[string, []byte]) error
	Destroy()
}

var _ IDiskIO = new(mbtree.IO)

type Type int

const (
	MBtree Type = iota
)

func New(t Type) IDiskIO {
	switch t {
	case MBtree:
		return mbtree.NewMBtreeIO()
	default:
		panic("unknown disk_io type")
	}
}
