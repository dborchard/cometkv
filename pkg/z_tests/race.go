package tests

import (
	"cometkv/pkg/b_memtable"
	"fmt"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
	"time"
)

// Test6 Single Writer. Multi Reader
func Test6(
	newTable func(gcInterval, ttl time.Duration) memtable.IMemtable,
	t *testing.T,
) {
	tbl := newTable(15*time.Second, 60*time.Second)

	const n = 1000

	var wg sync.WaitGroup
	wg.Add(1)
	var oneIterationDone bool

	// Single Writer
	go func() {
		for i := 0; i < n; i++ {
			tbl.Put(fmt.Sprintf("%05d", i), newValue(i))
			if i == n-1 && !oneIterationDone {
				oneIterationDone = true
				i = 0
				wg.Done()
			}
		}
	}()
	wg.Wait()

	// Multi Reader
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			found := tbl.Get(fmt.Sprintf("%05d", i), time.Now())
			assert.Equal(t, newValue(i), found, "for %d", i)
			wg.Done()
		}(i)
	}
	wg.Wait()

	tbl.Close()
}

// Test7 Multi Writer. Multi Reader
func Test7(
	newTable func(gcInterval, ttl time.Duration) memtable.IMemtable,
	t *testing.T,
) {
	t.Skip("MW MR")
	tbl := newTable(1*time.Second, 1*time.Second)

	const n = 1000

	// Check values. Concurrent writes.
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			tbl.Put(fmt.Sprintf("%05d", i), newValue(i))
			wg.Done()
		}(i)
	}
	wg.Wait()

	// Check values. Concurrent reads.
	time.Sleep(3 * time.Second)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			found := tbl.Get(fmt.Sprintf("%05d", i), time.Now())
			if found != nil && len(found) > 0 {
				assert.Equal(t, newValue(i), found)
			}
			time.Sleep(1 * time.Second)
			wg.Done()
		}(i)
	}
	wg.Wait()

	tbl.Close()
}

func newValue(i int) []byte {
	return []byte(fmt.Sprintf("%05d", i))
}
