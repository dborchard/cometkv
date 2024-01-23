package mem_btree

import (
	"github.com/dborchard/cometkv/pkg/y/entry"
)
import "github.com/tidwall/btree"

type MinHeap []*btree.IterG[entry.Pair[[]byte, []byte]]

func (m *MinHeap) Len() int { return len(*m) }
func (m *MinHeap) Less(i, j int) bool {
	if (*m)[i].Item().Key == nil || len((*m)[i].Item().Key) == 0 {
		return true
	}
	if (*m)[j].Item().Key == nil || len((*m)[j].Item().Key) == 0 {
		return false
	}
	return entry.CompareKeys((*m)[i].Item().Key, (*m)[j].Item().Key) < 0
}
func (m *MinHeap) Swap(i, j int) { (*m)[i], (*m)[j] = (*m)[j], (*m)[i] }

func (m *MinHeap) Push(x interface{}) {
	*m = append(*m, x.(*btree.IterG[entry.Pair[[]byte, []byte]]))
}

func (m *MinHeap) Pop() interface{} {
	old := *m
	n := len(old)
	x := old[n-1]
	old[n-1] = nil
	*m = old[0 : n-1]
	return x
}
