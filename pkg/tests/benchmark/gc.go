package main

import (
	"runtime"
)

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
