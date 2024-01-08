package segment_ring

import (
	"cometkv/cmd/server/k6/generator"
	"cometkv/pkg/common"
	"cometkv/pkg/common/timestamp"
	"cometkv/pkg/impl/vacuum_cow"
	"cometkv/pkg/tests"
	"container/list"
	"context"
	"fmt"
	"github.com/alphadose/zenq/v2"
	"github.com/panjf2000/ants/v2"
	"github.com/tidwall/btree"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func Test1(t *testing.T) {
	tests.Test1(func(gcInterval, ttl time.Duration) common.IEphemeralMemtable {
		ctx := context.Background()
		return New(gcInterval, ttl, true, ctx)
	}, t)
}

func Test2(t *testing.T) {
	tests.Test2(func(gcInterval, ttl time.Duration) common.IEphemeralMemtable {
		ctx := context.Background()
		return New(gcInterval, ttl, true, ctx)
	}, t)
}

func Test3(t *testing.T) {
	tests.Test3(func(gcInterval, ttl time.Duration) common.IEphemeralMemtable {
		ctx := context.Background()
		return New(gcInterval, ttl, true, ctx)
	}, t)
}

func Test4(t *testing.T) {
	tests.Test4(func(gcInterval, ttl time.Duration) common.IEphemeralMemtable {
		ctx := context.Background()
		return New(gcInterval, ttl, true, ctx)
	}, t)
}

func Test5(t *testing.T) {
	tests.Test5(func(gcInterval, ttl time.Duration) common.IEphemeralMemtable {
		ctx := context.Background()
		return New(gcInterval, ttl, true, ctx)
	}, t)
}

func Test6(t *testing.T) {
	tests.Test6(func(gcInterval, ttl time.Duration) common.IEphemeralMemtable {
		ctx := context.Background()
		return New(gcInterval, ttl, true, ctx)
	}, t)
}

func Test7(t *testing.T) {
	tests.Test7(func(gcInterval, ttl time.Duration) common.IEphemeralMemtable {
		ctx := context.Background()
		return New(gcInterval, ttl, true, ctx)
	}, t)
}

func Test8(t *testing.T) {
	tests.Test8(func(gcInterval, ttl time.Duration) common.IEphemeralMemtable {
		ctx := context.Background()
		return New(gcInterval, ttl, true, ctx)
	}, t)
}

func Test9(t *testing.T) {
	tests.Test9(func(gcInterval, ttl time.Duration) common.IEphemeralMemtable {
		ctx := context.Background()
		return New(gcInterval, ttl, true, ctx)
	}, t)
}

func Test10(t *testing.T) {
	tests.Test10(func(gcInterval, ttl time.Duration) common.IEphemeralMemtable {
		ctx := context.Background()
		return New(gcInterval, ttl, true, ctx)
	}, t)
}

func Test11(t *testing.T) {
	tests.Test11(func(gcInterval, ttl time.Duration) common.IEphemeralMemtable {
		ctx := context.Background()
		return New(gcInterval, ttl, true, ctx)
	}, t)
}

func BenchmarkAll(b *testing.B) {

	// SG
	sg := New(10*time.Second, 60*time.Second, true, context.Background())
	b.Run("SG Insert", func(b *testing.B) {
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			sg.Put("Arjun", []byte("Arjun"))
		}
	})

	// BTree
	tree := btree.NewBTreeG(func(a, b common.Pair[[]byte, *list.Element]) bool {
		return common.CompareKeys(a.Key, b.Key) < 0
	})
	internalKey := common.KeyWithTs([]byte("Arjun"), timestamp.ToUnit64(time.Now()))
	entry := common.Pair[[]byte, *list.Element]{Key: internalKey, Val: nil}
	b.Run("Tree Insert", func(b *testing.B) {
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			tree.Set(entry)
		}
	})

	// WG
	var wg1 sync.WaitGroup
	b.Run("WG Add", func(b *testing.B) {
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			wg1.Add(1)
		}
	})

	// Channel
	ch := make(chan *common.Pair[[]byte, *list.Element], 100)
	s := &common.Pair[[]byte, *list.Element]{}

	b.Run("Channel Insert", func(b *testing.B) {
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			ch <- s
			<-ch
		}
	})

	// Channel
	zCh := zenq.New[*common.Pair[[]byte, *list.Element]](100)

	b.Run("ZenQ", func(b *testing.B) {
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			zCh.Write(&entry)
			zCh.Read()
		}
	})

	// Go Routine
	var wg2 sync.WaitGroup
	b.Run("Go Routine", func(b *testing.B) {
		wg2.Add(b.N)
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			go func() {
				wg2.Done()
			}()
		}
		wg2.Wait()
	})

	// Ant Goroutine
	var wg3 sync.WaitGroup
	p, _ := ants.NewPool(10000, ants.WithPreAlloc(true))
	b.Run("Ant Thread Pool", func(b *testing.B) {
		wg3.Add(b.N)
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			_ = p.Submit(func() {
				wg3.Done()
			})
		}
		wg3.Wait()
	})
}

func BenchmarkSegmentRing_Put(b *testing.B) {

	keygenIntMax := generator.Build(generator.UNIFORM, 1, 1000_000)
	randSeq := rand.New(rand.NewSource(time.Now().UnixNano()))
	val := make([]byte, 1000)

	cow := vacuum_cow.New(10*time.Second, 60*time.Second, true, context.Background())
	b.Run("CoW Put", func(b *testing.B) {
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			key := fmt.Sprintf("%d", keygenIntMax.Next(randSeq))
			rand.Read(val)
			cow.Put(key, val)
		}
	})
	cow.Close()

	sg := New(10*time.Second, 60*time.Second, true, context.Background())
	b.Run("Segment Ring Put", func(b *testing.B) {
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			key := fmt.Sprintf("%d", keygenIntMax.Next(randSeq))
			rand.Read(val)
			sg.Put(key, val)
		}
	})
	sg.Close()

}

func Benchmark_Atomic_Lock(b *testing.B) {

	num := atomic.Int64{}
	num.Store(1)
	b.Run("Atomic", func(b *testing.B) {
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			if num.Load() > 0 {

			}
		}
	})

	lock := sync.Mutex{}
	b.Run("Lock", func(b *testing.B) {
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			lock.Lock()
			lock.Unlock()
		}
	})

}
