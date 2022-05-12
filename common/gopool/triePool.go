package gopool

import (
	"runtime"
	"time"

	"github.com/panjf2000/ants/v2"
)

var (
	// Init a instance pool when importing ants.
	tirePool, _ = ants.NewPool(ants.DefaultAntsPoolSize, ants.WithExpiryDuration(10*time.Second))
)

// Submit submits a task to pool.
func TrieSubmit(task func()) error {
	return defaultPool.Submit(task)
}

// Running returns the number of the currently running goroutines.
func TrieRunning() int {
	return defaultPool.Running()
}

// Cap returns the capacity of this default pool.
func TrieCap() int {
	return defaultPool.Cap()
}

// Free returns the available goroutines to work.
func TrieFree() int {
	return defaultPool.Free()
}

// Release Closes the default pool.
func TrieRelease() {
	defaultPool.Release()
}

// Reboot reboots the default pool.
func TrieReboot() {
	defaultPool.Reboot()
}

func TrieTrieThreads(tasks int) int {
	threads := tasks / minNumberPerTask
	if threads > runtime.NumCPU() {
		threads = runtime.NumCPU()
	} else if threads == 0 {
		threads = 1
	}
	return threads
}
