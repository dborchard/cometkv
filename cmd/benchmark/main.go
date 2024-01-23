package main

import (
	"context"
	"fmt"
	"github.com/arjunsk/cometkv/cmd/benchmark/generator"
	kv "github.com/arjunsk/cometkv/pkg/a_kv"
	memtable "github.com/arjunsk/cometkv/pkg/b_memtable"
	sstio "github.com/arjunsk/cometkv/pkg/c_sst_storage"
	lotsaa "github.com/arjunsk/lotsaa"
	"math/rand"
	"runtime"

	//_ "net/http/pprof"
	"os"
	"sync/atomic"
	"time"
)

var globalInsertCounter atomic.Int64
var globalMissCounter atomic.Int64

func main() {

	lotsaa.Output = os.Stdout

	keyRange := int64(10_000_000) // 10M
	scanWidth := 1000             // 1, 100, 1000
	variableWidth := true         //true or false

	testDuration := 10 * time.Minute // 10min, 11min, 12min
	scanThreadCount := 16            // 16, 32, 64, 128, 254

	gcInterval := 30 * time.Second   // 5sec, 30sec, 1m
	ttl := 3 * time.Minute           // 3min
	flushInterval := 1 * time.Minute // 1min

	PrintIP()
	fmt.Printf("** New Run %s ** \n", time.Now().Format("2006_01_02_15_04_05"))
	fmt.Printf("Scan Width = %d, Variable Width = %t \n", scanWidth, variableWidth)

	fmt.Printf("GC used %s @ time %s \n", gcInterval, time.Now().Format("2006_01_02_15_04_05"))
	for tc := scanThreadCount; tc <= 1024; tc = tc * 2 {
		RangeScanBenchTest(gcInterval, ttl, flushInterval, testDuration, memtable.HWTCoWBTree, keyRange, scanWidth, tc, variableWidth)
		RangeScanBenchTest(gcInterval, ttl, flushInterval, testDuration, memtable.MoRCoWBTree, keyRange, scanWidth, tc, variableWidth)
		RangeScanBenchTest(gcInterval, ttl, flushInterval, testDuration, memtable.SegmentRing, keyRange, scanWidth, tc, variableWidth)
		RangeScanBenchTest(gcInterval, ttl, flushInterval, testDuration, memtable.VacuumCoW, keyRange, scanWidth, tc, variableWidth)

		RangeScanBenchTest(gcInterval, ttl, flushInterval, testDuration, memtable.MoRBTree, keyRange, scanWidth, tc, variableWidth)
		RangeScanBenchTest(gcInterval, ttl, flushInterval, testDuration, memtable.HWTBTree, keyRange, scanWidth, tc, variableWidth)
		RangeScanBenchTest(gcInterval, ttl, flushInterval, testDuration, memtable.VacuumBTree, keyRange, scanWidth, tc, variableWidth)
		RangeScanBenchTest(gcInterval, ttl, flushInterval, testDuration, memtable.VacuumSkipList, keyRange, scanWidth, tc, variableWidth)

		fmt.Printf("Batch Completed %s \n", time.Now().Format("2006_01_02_15_04_05"))
		fmt.Println("----------------------------------------------------------------------------------------------")
	}

}

func RangeScanBenchTest(gcInterval, ttl, flushInterval, testDuration time.Duration, typ memtable.Typ, keyRange int64, scanWidth, scanThreadCount int, variableWidth bool) {

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)

	// 1. Build MemTable
	kvStore := kv.NewCometKV(ctx, typ, sstio.MBtree, gcInterval, ttl, flushInterval)
	tableName := kvStore.MemTableName()

	// 2.a Start Single Writer to the kvStore
	SingleWriter(keyRange, kvStore, ctx)

	// 2.b Make sure table is half filled (Wait for 1m15s)
	time.Sleep(time.Minute)
	time.Sleep(15 * time.Second)

	// 2.c Reset Counters & Stats
	globalInsertCounter.Store(0)
	globalMissCounter.Store(0)

	// 4. Multi Reader
	MultiReader(kvStore, tableName, keyRange, scanWidth, scanThreadCount, testDuration, variableWidth)
	cancel()
	time.Sleep(5 * time.Second)
	kvStore.Close()

	// 5. Print Custom Global Stats
	fmt.Printf(" I = %d M=%d\n", globalInsertCounter.Load(), globalMissCounter.Load())

	// 6. Reset Counters & Stats
	globalInsertCounter.Store(0)
	globalMissCounter.Store(0)

	// 7. GC
	startGc()
}

func SingleWriter(keyRange int64, kvStore kv.KV, ctx context.Context) {
	randSeq := rand.New(rand.NewSource(time.Now().UnixNano()))
	keygen := generator.Build(generator.UNIFORM, 1, keyRange)

	val := make([]byte, 1024)
	go func() {
		for {

			select {
			case <-ctx.Done():
				return
			default:
				// key length 16 --> cache padding improvement
				key := fmt.Sprintf("%16d", keygen.Next(randSeq))
				rand.Read(val)

				kvStore.Put(key, val)

				globalInsertCounter.Add(1)
			}
		}
	}()
}

func MultiReader(kvStore kv.KV, tableName string, keyRange int64, scanWidth, threadCount int, testDuration time.Duration, variableWidth bool) {
	fmt.Print(tableName, "			")

	keyGen := generator.Build(generator.UNIFORM, 1, keyRange)
	scanWidthGen := generator.Build(generator.UNIFORM, 1, int64(scanWidth))

	lotsaa.Time(testDuration, threadCount, func(threadRand *rand.Rand, threadIdx int) {
		// key length 16 --> cache padding improvement
		key := fmt.Sprintf("%16d", keyGen.Next(threadRand))

		var count int
		if variableWidth {
			count = int(scanWidthGen.Next(threadRand))
		} else {
			count = scanWidth
		}

		records := kvStore.Scan(key, count, time.Now())
		if len(records) != count {
			globalMissCounter.Add(1)
		}
	})
}

func startGc() {
	runtime.GC()
}
