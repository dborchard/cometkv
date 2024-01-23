package main

import (
	"context"
	"fmt"
	"github.com/arjunsk/cometkv/cmd/benchmark/generator"
	"github.com/arjunsk/cometkv/cmd/benchmark/lotsaa"
	kv "github.com/arjunsk/cometkv/pkg/a_kv"
	sstio "github.com/arjunsk/cometkv/pkg/c_sst_storage"
	"io/ioutil"
	"math/rand"
	"net/http"
	"runtime"

	//_ "net/http/pprof"
	"os"
	"sync/atomic"
	"time"
)

var globalInsertCounter atomic.Int64
var globalMissCounter atomic.Int64
var globalLongRangeScanCount atomic.Int64
var globalLongRangeScanDuration = time.Duration(0)

func init() {

	//runtime.SetMutexProfileFraction(1)
	//runtime.SetBlockProfileRate(1)
	//
	//// Server for pprof
	//go func() {
	//	fmt.Println(http.ListenAndServe("localhost:6060", nil))
	//}()

}

func main() {

	lotsaa.Output = os.Stdout
	//lotsaa.MemUsage = true

	keysSpace := int64(10_000_000)       // 10M
	scanWidth := 1000                    // 1, 100, 1000
	ttl := 3 * time.Minute               // 3min
	testDuration := 10 * time.Minute     // 10min, 11min, 12min
	scanThreadCount := 16                // 16, 32, 64, 128, 254
	gcInterval := 30 * time.Second       // 5sec, 30sec, 1m
	startLongRangeScan := true           // true
	longRangeDuration := 1 * time.Minute // 1min
	variableWidth := true                //true or false

	PrintIP()
	fmt.Printf("** New Run %s ** \n", time.Now().Format("2006_01_02_15_04_05"))
	fmt.Printf("Scan Width = %d, Variable Width = %t \n", scanWidth, variableWidth)

	fmt.Printf("GC used %s @ time %s \n", gcInterval, time.Now().Format("2006_01_02_15_04_05"))
	for tc := scanThreadCount; tc <= 1024; tc = tc * 2 {
		RangeScanBenchTest(gcInterval, ttl, longRangeDuration, testDuration, memtable.HWTCoWBTree, keysSpace, scanWidth, tc, startLongRangeScan, variableWidth)
		RangeScanBenchTest(gcInterval, ttl, longRangeDuration, testDuration, memtable.MoRCoWBTree, keysSpace, scanWidth, tc, startLongRangeScan, variableWidth)
		RangeScanBenchTest(gcInterval, ttl, longRangeDuration, testDuration, memtable.SegmentRing, keysSpace, scanWidth, tc, startLongRangeScan, variableWidth)
		RangeScanBenchTest(gcInterval, ttl, longRangeDuration, testDuration, memtable.VacuumCoW, keysSpace, scanWidth, tc, startLongRangeScan, variableWidth)

		RangeScanBenchTest(gcInterval, ttl, longRangeDuration, testDuration, memtable.MoRBTree, keysSpace, scanWidth, tc, startLongRangeScan, variableWidth)
		RangeScanBenchTest(gcInterval, ttl, longRangeDuration, testDuration, memtable.HWTBTree, keysSpace, scanWidth, tc, startLongRangeScan, variableWidth)
		RangeScanBenchTest(gcInterval, ttl, longRangeDuration, testDuration, memtable.VacuumBTree, keysSpace, scanWidth, tc, startLongRangeScan, variableWidth)
		RangeScanBenchTest(gcInterval, ttl, longRangeDuration, testDuration, memtable.VacuumSkipList, keysSpace, scanWidth, tc, startLongRangeScan, variableWidth)

		fmt.Printf("Batch Completed %s \n", time.Now().Format("2006_01_02_15_04_05"))
		fmt.Println("----------------------------------------------------------------------------------------------")
	}

}

func RangeScanBenchTest(gcInterval, ttl, longRangeDuration, testDuration time.Duration, typ memtable.MemtableTyp, keysSpace int64, scanWidth, scanThreadCount int, startLongRangeScan, variableWidth bool) {

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)

	// 1. Build MemTable
	tree := kv.NewCometKV(ctx, typ, sstio.MBtree, gcInterval, ttl)
	tableName := tree.Name()

	// 2.a Start Single Writer to the tree
	SingleWriter(keysSpace, tree, ctx)

	// 2.b Make sure table is half filled (Wait for 1m15s)
	time.Sleep(time.Minute)
	time.Sleep(15 * time.Second)

	// 2.c Reset Counters & Stats
	globalInsertCounter.Store(0)
	globalMissCounter.Store(0)
	globalLongRangeScanCount.Store(0)
	globalLongRangeScanDuration = time.Duration(0)

	// 4. Multi Reader
	MultiReader(tree, tableName, keysSpace, scanWidth, scanThreadCount, testDuration, gcInterval, variableWidth)
	cancel()
	time.Sleep(5 * time.Second)
	tree.Close()

	// 5. Print Custom Global Stats
	fmt.Printf(" I = %d M=%d LRD=%s LRC=%d\n",
		globalInsertCounter.Load(),
		globalMissCounter.Load(),
		globalLongRangeScanDuration,
		globalLongRangeScanCount.Load())

	// 6. Reset Counters & Stats
	globalInsertCounter.Store(0)
	globalMissCounter.Store(0)

	// 7. GC
	startGc()
}

func SingleWriter(keySpace int64, tree kv.KV, ctx context.Context) {
	randSeq := rand.New(rand.NewSource(time.Now().UnixNano()))
	keygen := generator.Build(generator.UNIFORM, 1, keySpace)

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

				tree.Put(key, val)

				globalInsertCounter.Add(1)
			}
		}
	}()
}

func MultiReader(tree kv.KV, tableName string, keySpace int64, scanWidth, threadCount int, testDuration, gcInterval time.Duration, variableWidth bool) {
	fmt.Print(tableName, "			")

	keyGen := generator.Build(generator.UNIFORM, 1, keySpace)
	scanWidthGen := generator.Build(generator.UNIFORM, 1, int64(scanWidth))

	lotsaa.Ops(testDuration, threadCount,
		func(threadRand *rand.Rand, threadIdx int) {
			// key length 16 --> cache padding improvement
			key := fmt.Sprintf("%16d", keyGen.Next(threadRand))

			var count int
			if variableWidth {
				count = int(scanWidthGen.Next(threadRand))
			} else {
				count = scanWidth
			}

			records := tree.Scan(key, count, time.Now())
			if len(records) != count {
				globalMissCounter.Add(1)
			}
		},
	)
}

func PrintIP() {
	resp, err := http.Get("https://icanhazip.com/")
	if err != nil {
		fmt.Println("Error fetching public IP:", err)
		return
	}
	defer resp.Body.Close()

	ip, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("Error reading response body:", err)
		return
	}

	fmt.Println("EC2 Instance Public IP:", string(ip))
}

func startGc() {
	//var garC debug.GCStats
	//debug.ReadGCStats(&garC)
	//
	////fmt.Printf("\nLastGC:\t%s", garC.LastGC)         // time of last collection
	//fmt.Printf("\nNumGC:\t%d", garC.NumGC)           // number of garbage collections
	//fmt.Printf("\nPauseTotal:\t%s", garC.PauseTotal) // total pause for all collections
	////fmt.Printf("\nPause:\t%s", garC.Pause)           // pause history, most recent first
	//fmt.Println()

	runtime.GC()
}
