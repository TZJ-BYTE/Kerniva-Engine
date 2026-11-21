// Command timing follows the platform clock used by scripts/bench.
package main

import (
	"syscall"
	"time"
	"unsafe"
)

var (
	kernel32         = syscall.NewLazyDLL("kernel32.dll")
	queryCounter     = kernel32.NewProc("QueryPerformanceCounter")
	queryFrequency   = kernel32.NewProc("QueryPerformanceFrequency")
	counterFrequency = readFrequency()
)

func readFrequency() int64 {
	var frequency int64
	if ok, _, _ := queryFrequency.Call(uintptr(unsafe.Pointer(&frequency))); ok == 0 || frequency <= 0 {
		panic("QueryPerformanceFrequency failed")
	}
	return frequency
}

func clockNow() int64 {
	var counter int64
	if ok, _, _ := queryCounter.Call(uintptr(unsafe.Pointer(&counter))); ok == 0 {
		panic("QueryPerformanceCounter failed")
	}
	return counter
}

func clockElapsed(start int64) time.Duration {
	ticks := clockNow() - start
	return time.Duration(ticks/counterFrequency)*time.Second + time.Duration(ticks%counterFrequency)*time.Second/time.Duration(counterFrequency)
}
