package disk_store

import (
	common "cometkv/pkg/common"
	"cometkv/pkg/d_diskio/mbtree"
	"time"
)

type DiskIO interface {
	Scan(startKey string, count int, snapshotTs time.Time) []common.Pair[string, []byte]
	Create(records []common.Pair[string, []byte]) error
	Destroy() error
}

var _ DiskIO = new(mbtree.IO)
