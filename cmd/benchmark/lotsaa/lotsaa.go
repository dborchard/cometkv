package lotsaa

import (
	"fmt"
	"io"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// Output is used to print elased time and ops/sec
var Output io.Writer

// MemUsage is used to output the memory usage
var MemUsage bool

// Ops executed a number of operations over a multiple goroutines.
// duration is the total period for continuously running the Operation.
// threads is the number goroutines.
// op is the operation function
// TODO: accept ctx
func Ops(duration time.Duration, threads int, op func(threadRand *rand.Rand, threadIdx int)) {

	var start time.Time
	var wg sync.WaitGroup
	wg.Add(threads)
	var ms1 runtime.MemStats
	output := Output
	if output != nil {
		if MemUsage {
			runtime.GC()
			runtime.ReadMemStats(&ms1)
		}
		start = time.Now()
	}

	var totalCount atomic.Int64
	for i := 0; i < threads; i++ {

		go func(i int) {
			ticker := time.NewTicker(duration)
			defer ticker.Stop()

			randGen := rand.New(rand.NewSource(time.Now().UnixNano()))
			for {
				select {
				case <-ticker.C:
					wg.Done()
					return
				default:
					{
						op(randGen, i)
						totalCount.Add(1)
					}
				}
			}
		}(i)
	}
	wg.Wait()

	if output != nil {
		dur := time.Since(start)
		var alloc uint64
		if MemUsage {
			runtime.GC()
			var ms2 runtime.MemStats
			runtime.ReadMemStats(&ms2)
			if ms1.HeapAlloc > ms2.HeapAlloc {
				alloc = 0
			} else {
				alloc = ms2.HeapAlloc - ms1.HeapAlloc
			}
		}
		WriteOutput(output, totalCount.Load(), threads, dur, alloc)
	}
}

func commaize(n int64) string {
	s1, s2 := fmt.Sprintf("%d", n), ""
	for i, j := len(s1)-1, 0; i >= 0; i, j = i-1, j+1 {
		if j%3 == 0 && j != 0 {
			s2 = "," + s2
		}
		s2 = string(s1[i]) + s2
	}
	return s2
}

// WriteOutput writes an output line to the specified writer
func WriteOutput(w io.Writer, count int64, threads int, elapsed time.Duration, alloc uint64) {
	fmt.Fprintf(w, "%d threads R = %s", threads, commaize(int64(float64(count)/elapsed.Seconds())))
}
