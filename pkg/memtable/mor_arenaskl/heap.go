package mor_cow

import (
	"github.com/dborchard/cometkv/pkg/memtable/mor_arenaskl/arenaskl"
	"github.com/dborchard/cometkv/pkg/y/entry"
)

type MinHeap []*arenaskl.Iterator

func (m MinHeap) Len() int { return len(m) }
func (m MinHeap) Less(i, j int) bool {
	if m[i].Key() == nil || len(m[i].Key()) == 0 {
		return true
	}
	if m[j].Key() == nil || len(m[j].Key()) == 0 {
		return false
	}
	return entry.CompareKeys(m[i].Key(), m[j].Key()) < 0
}
func (m MinHeap) Swap(i, j int) { m[i], m[j] = m[j], m[i] }

func (m *MinHeap) Push(x interface{}) {
	*m = append(*m, x.(*arenaskl.Iterator))
}

func (m *MinHeap) Pop() interface{} {
	old := *m
	n := len(old)
	x := old[n-1]
	old[n-1] = nil
	*m = old[0 : n-1]
	return x
}
