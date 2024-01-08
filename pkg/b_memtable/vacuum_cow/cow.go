package vacuum_cow

import (
	"github.com/tidwall/btree"
	"sync"
	"sync/atomic"
)

type BTreeGCoW[T any] struct {
	sync.Mutex
	state atomic.Pointer[btree.BTreeG[T]]
}

type IBTreeGCoW[T any] interface {
	Set(item T) (T, bool)
	Delete(key T) (T, bool)
	Ascend(pivot T, iter func(item T) bool)
	Len() int
	Scan(iter func(item T) bool)
}

var _ IBTreeGCoW[any] = new(BTreeGCoW[any])

func NewBTreeGCoW[T any](less func(a, b T) bool) *BTreeGCoW[T] {
	r := BTreeGCoW[T]{}
	r.state.Store(btree.NewBTreeG[T](less))
	return &r
}

func (tr *BTreeGCoW[T]) Set(item T) (T, bool) {
	tr.Lock() // To serialize Set and Delete

	newState := tr.state.Load().Copy()
	res1, res2 := newState.Set(item)
	tr.state.Store(newState)

	tr.Unlock()
	return res1, res2
}

func (tr *BTreeGCoW[T]) Delete(key T) (T, bool) {
	tr.Lock()

	newState := tr.state.Load().Copy()
	res1, res2 := newState.Delete(key)
	tr.state.Store(newState)

	tr.Unlock()
	return res1, res2

}

func (tr *BTreeGCoW[T]) Ascend(pivot T, iter func(item T) bool) {
	tr.state.Load().Ascend(pivot, iter)
}

func (tr *BTreeGCoW[T]) Len() int {
	return tr.state.Load().Len()
}

func (tr *BTreeGCoW[T]) Scan(iter func(item T) bool) {
	tr.state.Load().Scan(iter)
}

func (tr *BTreeGCoW[T]) Clear() {
	tr.state.Load().Clear()
}
