// Copyright 2024 The go-ethereum Authors
// This file is part of go-ethereum.
//
// go-ethereum is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// go-ethereum is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with go-ethereum. If not, see <http://www.gnu.org/licenses/>.

// state-perf is a CLI-based performance testing tool for PebbleDB state operations.
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"math"
	mathrand "math/rand"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum/cmd/utils"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/ethdb/pebble"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/node"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/triedb"
	"github.com/ethereum/go-ethereum/triedb/pathdb"
	"github.com/urfave/cli/v2"
)

const version = "1.0.0"

type PerfConfig struct {
	TestCaseDir       string
	BenchDBPath       string
	BatchSize         uint64
	ReadRatio         float64
	WriteRatio        float64
	UpdateRatio       float64
	NumThreads        int
	RuntimeDur        time.Duration
	MetricsAddr       string
	MetricsPort       int
	CacheSize         int // Database cache size in MB
	Handles           int // Number of file descriptor handles
	SnapReadBatchSize int // Number of snap KVs to read per batch
}

type DataType int

const (
	AccountTries DataType = iota
	StorageTries
	AccountSnaps
	StorageSnaps
)

type KeyValue struct {
	Key   []byte
	Value []byte
	Type  DataType
}

type DataSet struct {
	AccountTries []KeyValue
	StorageTries []KeyValue
	AccountSnaps []KeyValue
	StorageSnaps []KeyValue
}

type Task struct {
	MixedReadKVs []KeyValue // Mixed type reads (AccountTries + StorageTries)
	SnapReadKVs  []KeyValue // Snap type reads (AccountSnaps + StorageSnaps)
	UpdateKVs    []KeyValue
	WriteKVs     []KeyValue
}

type PerfRunner struct {
	dataSet  *DataSet
	db       *pebble.Database
	config   PerfConfig
	taskChan chan *Task
	ctx      *cli.Context     // CLI context for database operations
	chainDB  ethdb.Database   // Chain database for hash calculations
	stack    *node.Node       // Node stack for chainDB
	trieDB   *triedb.Database // Cached trie database
	theTrie  *trie.Trie       // Cached trie for hash calculations

	// Statistics
	blockHeight     uint64
	totalReadOps    int64
	totalWriteOps   int64
	totalUpdateOps  int64
	totalReadTime   time.Duration
	totalWriteTime  time.Duration
	totalUpdateTime time.Duration
	lastStatTime    time.Time
	totalBatchCount int64 // Total number of batches processed

	// Separate read statistics for mixed and snap types
	totalMixedReadOps  int64
	totalSnapReadOps   int64
	totalMixedReadTime time.Duration
	totalSnapReadTime  time.Duration

	// Update batch statistics - for 230-256MB accumulated batch write
	totalUpdateKVs        int64      // Total number of KVs in update batches
	totalUpdateSize       int64      // Total size of update batches in bytes
	accumulatedUpdateKVs  []KeyValue // In-memory accumulation for 230-256MB batch
	accumulatedUpdateSize int64      // Current accumulated size in bytes
	updateMutex           sync.Mutex // Protect accumulated update data

	// Write batch statistics
	totalBatchSize   int64 // Total size of all write batches
	totalBatchWrites int64 // Total number of write batches executed

	// Hash calculation statistics
	totalHashOps    int64
	totalHashTime   time.Duration
	totalCommitTime time.Duration // Total commit time for average calculation
	trieDir         string        // Directory for trie operations

	// For interval TPS calculation
	lastReadOps    int64
	lastWriteOps   int64
	lastUpdateOps  int64
	lastBatchCount int64
	lastUpdateKVs  int64 // Last update KV count for interval calculation
	lastUpdateSize int64 // Last update size for interval calculation
}

func main() {
	var config PerfConfig

	app := &cli.App{
		Name:    "",
		Usage:   "A CLI-based performance testing tool for PebbleDB state operations",
		Version: version,
		Commands: []*cli.Command{
			{
				Name:  "press-test",
				Usage: "Press test with random state data operations",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:        "testcase",
						Aliases:     []string{"tc"},
						Usage:       "Path to test-case pebble database directory",
						Value:       "test-case",
						Destination: &config.TestCaseDir,
					},
					&cli.StringFlag{
						Name:        "bench-db",
						Aliases:     []string{"bdb"},
						Usage:       "Path for benchmark pebble database",
						Value:       "bench-pebble",
						Destination: &config.BenchDBPath,
					},
					&cli.Uint64Flag{
						Name:        "batch",
						Aliases:     []string{"b"},
						Usage:       "Batch size for task generation",
						Value:       5000,
						Destination: &config.BatchSize,
					},
					&cli.Float64Flag{
						Name:        "read-ratio",
						Aliases:     []string{"rr"},
						Usage:       "Read operations ratio (0.0-1.0)",
						Value:       0.6,
						Destination: &config.ReadRatio,
					},
					&cli.Float64Flag{
						Name:        "write-ratio",
						Aliases:     []string{"wr"},
						Usage:       "Write operations ratio (0.0-1.0)",
						Value:       0.2,
						Destination: &config.WriteRatio,
					},
					&cli.Float64Flag{
						Name:        "update-ratio",
						Aliases:     []string{"ur"},
						Usage:       "Update operations ratio (0.0-1.0)",
						Value:       0.2,
						Destination: &config.UpdateRatio,
					},
					&cli.IntFlag{
						Name:        "threads",
						Aliases:     []string{"t"},
						Usage:       "Number of worker threads",
						Value:       1,
						Destination: &config.NumThreads,
					},
					&cli.DurationFlag{
						Name:        "runtime",
						Aliases:     []string{"rt"},
						Usage:       "Duration to run the benchmark",
						Value:       60 * time.Second,
						Destination: &config.RuntimeDur,
					},
					&cli.StringFlag{
						Name:        "metrics.addr",
						Aliases:     []string{"ma"},
						Usage:       "Metrics address",
						Value:       "127.0.0.1",
						Destination: &config.MetricsAddr,
					},
					&cli.IntFlag{
						Name:        "metrics.port",
						Aliases:     []string{"mp"},
						Usage:       "Metrics HTTP server listening port",
						Value:       8545,
						Destination: &config.MetricsPort,
					},
					&cli.IntFlag{
						Name:        "cache",
						Usage:       "Database cache size in MB",
						Value:       4096,
						Destination: &config.CacheSize,
					},
					&cli.IntFlag{
						Name:        "handles",
						Usage:       "Number of file descriptor handles",
						Value:       32766,
						Destination: &config.Handles,
					},
					&cli.IntFlag{
						Name:        "snapbatch",
						Aliases:     []string{"srb"},
						Usage:       "Number of snap KVs to read per batch operation",
						Value:       600,
						Destination: &config.SnapReadBatchSize,
					},
				},
				Before: func(c *cli.Context) error {
					// Validate ratios sum to 1.0
					total := config.ReadRatio + config.WriteRatio + config.UpdateRatio
					if math.Abs(total-1.0) > 0.01 {
						return fmt.Errorf("read-ratio + write-ratio + update-ratio must equal 1.0, got %.2f", total)
					}
					return nil
				},
				Action: func(c *cli.Context) error {
					return runPerfTest(c, &config)
				},
			},
		},
		Action: func(c *cli.Context) error {
			fmt.Printf("State Performance Tool v%s\n", version)
			fmt.Printf("Use 'state-perf press-test' to run performance test\n")
			return nil
		},
	}

	// Setup logging
	log.SetDefault(log.NewLogger(log.NewTerminalHandlerWithLevel(os.Stderr, log.LevelInfo, true)))

	err := app.Run(os.Args)
	if err != nil {
		log.Crit("Application failed", "err", err)
	}
}

func runPerfTest(c *cli.Context, config *PerfConfig) error {
	log.Info("Starting state performance test",
		"testcase", config.TestCaseDir,
		"batch", config.BatchSize,
		"readRatio", config.ReadRatio,
		"writeRatio", config.WriteRatio,
		"updateRatio", config.UpdateRatio,
		"runtime", config.RuntimeDur)

	// Load data-set from test-case directory
	log.Info("Loading data-set from test-case directory", "path", config.TestCaseDir)
	dataSet, err := loadDataSet(config.TestCaseDir)
	if err != nil {
		return fmt.Errorf("failed to load data-set: %v", err)
	}

	log.Info("Data-set loaded successfully",
		"accountTries", len(dataSet.AccountTries),
		"storageTries", len(dataSet.StorageTries),
		"accountSnaps", len(dataSet.AccountSnaps),
		"storageSnaps", len(dataSet.StorageSnaps))

	// Create benchmark database
	benchDB, err := pebble.New(config.BenchDBPath, config.CacheSize, config.Handles, "chaindata", false)
	if err != nil {
		return fmt.Errorf("failed to create benchmark database: %v", err)
	}
	defer benchDB.Close()

	// Create and start performance runner
	runner := NewPerfRunner(dataSet, benchDB, *config, c)
	defer runner.Close() // Ensure cleanup

	ctx, cancel := context.WithTimeout(context.Background(), config.RuntimeDur)
	defer cancel()

	// Handle interrupt signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		select {
		case <-sigChan:
			log.Info("Received interrupt signal, shutting down...")
			cancel()
		case <-ctx.Done():
		}
	}()

	// Start performance test
	log.Info("Starting performance test")
	runner.Run(ctx)

	return nil
}

func loadDataSet(testCaseDir string) (*DataSet, error) {
	// Open test-case pebble database
	db, err := pebble.New(testCaseDir, 4096, 32766, "", true) // readonly
	if err != nil {
		return nil, fmt.Errorf("failed to open test-case database: %v", err)
	}
	defer db.Close()

	dataSet := &DataSet{}

	// Iterate through all keys and classify them
	iter := db.NewIterator(nil, nil)
	defer iter.Release()

	for iter.Next() {
		key := make([]byte, len(iter.Key()))
		value := make([]byte, len(iter.Value()))
		copy(key, iter.Key())
		copy(value, iter.Value())

		kv := KeyValue{Key: key, Value: value}

		// Classify data type based on key prefix
		switch {
		case rawdb.IsAccountTrieNode(key):
			kv.Type = AccountTries
			dataSet.AccountTries = append(dataSet.AccountTries, kv)
		case rawdb.IsStorageTrieNode(key):
			kv.Type = StorageTries
			dataSet.StorageTries = append(dataSet.StorageTries, kv)
		case bytes.HasPrefix(key, rawdb.SnapshotAccountPrefix) && len(key) == (len(rawdb.SnapshotAccountPrefix)+common.HashLength):
			kv.Type = AccountSnaps
			dataSet.AccountSnaps = append(dataSet.AccountSnaps, kv)
		case bytes.HasPrefix(key, rawdb.SnapshotStoragePrefix) && len(key) == (len(rawdb.SnapshotStoragePrefix)+2*common.HashLength):
			kv.Type = StorageSnaps
			dataSet.StorageSnaps = append(dataSet.StorageSnaps, kv)
		}
	}

	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("iterator error: %v", err)
	}

	// Log data set statistics
	fmt.Printf("Data set loaded successfully:\n")
	fmt.Printf("  Account Tries: %d\n", len(dataSet.AccountTries))
	fmt.Printf("  Storage Tries: %d\n", len(dataSet.StorageTries))
	fmt.Printf("  Account Snaps: %d\n", len(dataSet.AccountSnaps))
	fmt.Printf("  Storage Snaps: %d\n", len(dataSet.StorageSnaps))

	// Check if we have enough snap data for operations
	totalSnapData := len(dataSet.AccountSnaps) + len(dataSet.StorageSnaps)
	if totalSnapData == 0 {
		fmt.Printf("WARNING: No snapshot data found in test-case database. Snap read operations will be skipped.\n")
	} else {
		fmt.Printf("Snapshot data available: %d total entries\n", totalSnapData)
	}

	return dataSet, nil
}

func NewPerfRunner(dataSet *DataSet, db *pebble.Database, config PerfConfig, ctx *cli.Context) *PerfRunner {
	// Create a new node for chainDB
	stack, err := makeConfigNode(ctx, config.BenchDBPath)
	if err != nil {
		log.Crit("Failed to create node for chainDB", "err", err)
	}

	chainDB := utils.MakeChainDatabase(ctx, stack, true, false)

	return &PerfRunner{
		dataSet:      dataSet,
		db:           db,
		config:       config,
		taskChan:     make(chan *Task, 10),
		ctx:          ctx,
		chainDB:      chainDB,
		stack:        stack,
		lastStatTime: time.Now(),
	}
}

// Close cleans up PerfRunner resources
func (r *PerfRunner) Close() {
	// Flush any remaining accumulated updates before closing
	r.updateMutex.Lock()
	hasData := len(r.accumulatedUpdateKVs) > 0
	r.updateMutex.Unlock()

	if hasData {
		log.Info("Flushing remaining accumulated updates before shutdown")
		r.flushAccumulatedWrites()
	}

	if r.trieDB != nil {
		r.trieDB.Close()
	}
	if r.chainDB != nil {
		r.chainDB.Close()
	}
	if r.stack != nil {
		r.stack.Close()
	}
}

func (r *PerfRunner) Run(ctx context.Context) {
	// Start task generator
	go r.generateTasks(ctx)

	// Start performance test runner
	r.runInternal(ctx)
}

func (r *PerfRunner) generateTasks(ctx context.Context) {
	defer close(r.taskChan)

	for {
		select {
		case <-ctx.Done():
			return
		default:
			task := r.createTask()
			select {
			case r.taskChan <- task:
			case <-ctx.Done():
				return
			}
		}
	}
}

func (r *PerfRunner) createTask() *Task {
	task := &Task{}

	// Calculate total read count based on ratio
	quarterBatch := int(r.config.BatchSize) / 4
	allKVs := make([]KeyValue, 0, r.config.BatchSize)

	// Add 1/4 from each data type
	allKVs = append(allKVs, r.selectRandomKVs(r.dataSet.AccountTries, quarterBatch)...)
	allKVs = append(allKVs, r.selectRandomKVs(r.dataSet.StorageTries, quarterBatch)...)
	allKVs = append(allKVs, r.selectRandomKVs(r.dataSet.AccountSnaps, quarterBatch)...)
	allKVs = append(allKVs, r.selectRandomKVs(r.dataSet.StorageSnaps, quarterBatch)...)

	// Shuffle the combined KVs
	mathrand.Shuffle(len(allKVs), func(i, j int) {
		allKVs[i], allKVs[j] = allKVs[j], allKVs[i]
	})

	// Distribute according to configured ratios
	total := len(allKVs)
	readCount := int(float64(total) * r.config.ReadRatio)
	updateCount := int(float64(total) * r.config.UpdateRatio)
	writeCount := total - readCount - updateCount

	// Split read operations: half mixed (trie types), half snap types
	mixedReadCount := readCount / 2
	snapReadCount := readCount - mixedReadCount

	// Create mixed reads from trie types (AccountTries + StorageTries)
	mixedKVs := make([]KeyValue, 0, mixedReadCount)
	mixedAccountCount := mixedReadCount / 2
	mixedStorageCount := mixedReadCount - mixedAccountCount
	mixedKVs = append(mixedKVs, r.selectRandomKVs(r.dataSet.AccountTries, mixedAccountCount)...)
	mixedKVs = append(mixedKVs, r.selectRandomKVs(r.dataSet.StorageTries, mixedStorageCount)...)
	mathrand.Shuffle(len(mixedKVs), func(i, j int) {
		mixedKVs[i], mixedKVs[j] = mixedKVs[j], mixedKVs[i]
	})
	task.MixedReadKVs = mixedKVs

	// Create snap reads from snap types (AccountSnaps + StorageSnaps)
	// Use SnapReadBatchSize to control actual read count
	actualSnapReadCount := min(snapReadCount, r.config.SnapReadBatchSize)
	snapKVs := make([]KeyValue, 0, actualSnapReadCount)
	snapAccountCount := actualSnapReadCount / 2
	snapStorageCount := actualSnapReadCount - snapAccountCount

	// Debug info: show snap read calculation
	if snapReadCount == 0 || len(r.dataSet.AccountSnaps) == 0 && len(r.dataSet.StorageSnaps) == 0 {
		fmt.Printf("DEBUG: Snap read skipped - snapReadCount=%d, AccountSnaps=%d, StorageSnaps=%d, actualSnapReadCount=%d\n",
			snapReadCount, len(r.dataSet.AccountSnaps), len(r.dataSet.StorageSnaps), actualSnapReadCount)
	}

	snapKVs = append(snapKVs, r.selectRandomKVs(r.dataSet.AccountSnaps, snapAccountCount)...)
	snapKVs = append(snapKVs, r.selectRandomKVs(r.dataSet.StorageSnaps, snapStorageCount)...)
	mathrand.Shuffle(len(snapKVs), func(i, j int) {
		snapKVs[i], snapKVs[j] = snapKVs[j], snapKVs[i]
	})
	task.SnapReadKVs = snapKVs

	// Update KVs from all types
	task.UpdateKVs = allKVs[readCount : readCount+updateCount]

	// For WriteKVs, only use trie types (AccountTries and StorageTries)
	trieKVs := make([]KeyValue, 0, writeCount)
	accountTrieCount := writeCount / 2
	storageTrieCount := writeCount - accountTrieCount // Handle odd writeCount
	trieKVs = append(trieKVs, r.selectRandomKVs(r.dataSet.AccountTries, accountTrieCount)...)
	trieKVs = append(trieKVs, r.selectRandomKVs(r.dataSet.StorageTries, storageTrieCount)...)

	// Shuffle trie KVs and take only what we need
	mathrand.Shuffle(len(trieKVs), func(i, j int) {
		trieKVs[i], trieKVs[j] = trieKVs[j], trieKVs[i]
	})

	if len(trieKVs) > writeCount {
		task.WriteKVs = trieKVs[:writeCount]
	} else {
		task.WriteKVs = trieKVs
	}

	return task
}

func (r *PerfRunner) selectRandomKVs(source []KeyValue, count int) []KeyValue {
	if len(source) == 0 {
		return nil
	}

	result := make([]KeyValue, 0, count)
	for i := 0; i < count; i++ {
		idx := mathrand.Intn(len(source))
		result = append(result, source[idx])
	}
	return result
}

func (r *PerfRunner) runInternal(ctx context.Context) {
	startTime := time.Now()
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	// Hash calculation ticker - every 3 seconds
	hashTicker := time.NewTicker(3 * time.Second)
	defer hashTicker.Stop()

	for {
		select {
		case task := <-r.taskChan:
			if task == nil {
				fmt.Println("Task channel closed, shutting down")
				r.printAVGStat(startTime)
				r.printHashSummary()
				return
			}
			r.processTask(task)
			r.blockHeight++

			// Print average stats every 100 batches
			if r.blockHeight > 0 && r.blockHeight%100 == 0 {
				r.printAVGStat(startTime)
			}

		case <-ticker.C:
			r.printStat()

		case <-hashTicker.C:
			// Perform hash calculation every 3 seconds asynchronously
			go r.calculateHashRoot()

		case <-ctx.Done():
			fmt.Println("Context cancelled, shutting down")
			r.printAVGStat(startTime)
			r.printHashSummary()
			return
		}
	}
}

func (r *PerfRunner) processTask(task *Task) {

	// Process mixed read operations sequentially
	if len(task.MixedReadKVs) > 0 {
		readStart := time.Now()
		var readWG sync.WaitGroup
		r.processMixedReadsParallel(task.MixedReadKVs, &readWG)
		readWG.Wait()
		atomic.AddInt64((*int64)(&r.totalMixedReadTime), int64(time.Since(readStart)))
	}

	// Process snap read operations sequentially
	if len(task.SnapReadKVs) > 0 {
		readStart := time.Now()
		var readWG sync.WaitGroup
		r.processSnapReadsParallel(task.SnapReadKVs, &readWG)
		readWG.Wait()
		atomic.AddInt64((*int64)(&r.totalSnapReadTime), int64(time.Since(readStart)))
	}

	// Process update operations sequentially - individual db.Put for each KV
	// Note: time is now measured inside processIndividualUpdates for each db.Put
	if len(task.UpdateKVs) > 0 {
		r.processIndividualUpdates(task.UpdateKVs)
	}

	// Accumulate write operations in memory for 256MB batch write
	if len(task.WriteKVs) > 0 {
		writeStart := time.Now()
		r.accumulateWrites(task.WriteKVs)
		atomic.AddInt64((*int64)(&r.totalWriteTime), int64(time.Since(writeStart)))
	}

}

func (r *PerfRunner) processMixedReadsParallel(readKVs []KeyValue, wg *sync.WaitGroup) {
	numThreads := r.config.NumThreads
	if numThreads <= 0 {
		numThreads = 1
	}

	chunkSize := len(readKVs) / numThreads
	if chunkSize == 0 {
		chunkSize = 1
	}

	for i := 0; i < numThreads; i++ {
		start := i * chunkSize
		end := start + chunkSize
		if i == numThreads-1 {
			end = len(readKVs) // Last thread handles remaining items
		}
		if start >= len(readKVs) {
			break
		}

		wg.Add(1)
		go func(kvs []KeyValue) {
			defer wg.Done()
			localReadOps := int64(0)

			for _, kv := range kvs {
				_, err := r.db.Get(kv.Key)
				if err != nil {
					// Key might not exist, continue
				}
				localReadOps++
			}

			atomic.AddInt64(&r.totalMixedReadOps, localReadOps)
			atomic.AddInt64(&r.totalReadOps, localReadOps) // Keep total counter for compatibility
		}(readKVs[start:end])
	}
}

func (r *PerfRunner) processSnapReadsParallel(readKVs []KeyValue, wg *sync.WaitGroup) {
	numThreads := r.config.NumThreads
	if numThreads <= 0 {
		numThreads = 1
	}

	chunkSize := len(readKVs) / numThreads
	if chunkSize == 0 {
		chunkSize = 1
	}

	for i := 0; i < numThreads; i++ {
		start := i * chunkSize
		end := start + chunkSize
		if i == numThreads-1 {
			end = len(readKVs) // Last thread handles remaining items
		}
		if start >= len(readKVs) {
			break
		}

		wg.Add(1)
		go func(kvs []KeyValue) {
			defer wg.Done()
			localReadOps := int64(0)

			for _, kv := range kvs {
				_, err := r.db.Get(kv.Key)
				if err != nil {
					// Key might not exist, continue
				}
				localReadOps++
			}

			atomic.AddInt64(&r.totalSnapReadOps, localReadOps)
			atomic.AddInt64(&r.totalReadOps, localReadOps) // Keep total counter for compatibility
		}(readKVs[start:end])
	}
}

// processIndividualUpdates performs individual db.Put for each update KV
func (r *PerfRunner) processIndividualUpdates(updateKVs []KeyValue) {
	numThreads := r.config.NumThreads
	if numThreads <= 0 {
		numThreads = 1
	}

	chunkSize := len(updateKVs) / numThreads
	if chunkSize == 0 {
		chunkSize = 1
	}

	var wg sync.WaitGroup
	for i := 0; i < numThreads; i++ {
		start := i * chunkSize
		end := start + chunkSize
		if i == numThreads-1 {
			end = len(updateKVs) // Last thread handles remaining items
		}
		if start >= len(updateKVs) {
			break
		}

		wg.Add(1)
		go func(kvs []KeyValue) {
			defer wg.Done()
			localUpdateOps := int64(0)
			localUpdateSize := int64(0)
			localUpdateTime := int64(0)

			for _, kv := range kvs {
				// Modify value slightly for update - just append a random byte
				newValue := make([]byte, len(kv.Value)+1)
				copy(newValue, kv.Value)
				newValue[len(kv.Value)] = byte(mathrand.Intn(256))

				// Measure individual db.Put time
				putStart := time.Now()
				err := r.db.Put(kv.Key, newValue)
				putDuration := time.Since(putStart)

				if err != nil {
					log.Warn("Failed to update key", "err", err)
				} else {
					localUpdateOps++
					localUpdateSize += int64(len(kv.Key) + len(newValue))
					localUpdateTime += int64(putDuration)
				}
			}

			atomic.AddInt64(&r.totalUpdateOps, localUpdateOps)
			atomic.AddInt64(&r.totalUpdateKVs, localUpdateOps)
			atomic.AddInt64(&r.totalUpdateSize, localUpdateSize)
			atomic.AddInt64((*int64)(&r.totalUpdateTime), localUpdateTime)
		}(updateKVs[start:end])
	}
	wg.Wait()
}

// accumulateWrites accumulates write KVs in memory until 230-256MB, then triggers batch write
func (r *PerfRunner) accumulateWrites(writeKVs []KeyValue) {
	const minBatchSize = 230 * 1024 * 1024 // 230MB
	const maxBatchSize = 256 * 1024 * 1024 // 256MB

	// Prepare expanded KVs for accumulation
	expandedKVs := make([]KeyValue, 0, len(writeKVs))
	totalSize := int64(0)

	for _, kv := range writeKVs {
		// Generate new key with same prefix for write operations
		newKey := r.generateKeyWithSamePrefix(kv.Key)

		// Modify value to reach larger size for 256MB target
		// Expand each value by approximately 5x to reach target size
		originalSize := len(kv.Value)
		expandFactor := 5

		// Create a larger value by repeating the original data and adding random bytes
		newValue := make([]byte, 0, originalSize*expandFactor+256)

		// Repeat original value multiple times
		for i := 0; i < expandFactor; i++ {
			newValue = append(newValue, kv.Value...)
		}

		// Add additional random bytes to ensure size variation
		additionalBytes := make([]byte, 256)
		rand.Read(additionalBytes)
		newValue = append(newValue, additionalBytes...)

		expandedKV := KeyValue{
			Key:   newKey,
			Value: newValue,
			Type:  kv.Type,
		}
		expandedKVs = append(expandedKVs, expandedKV)
		totalSize += int64(len(newKey) + len(newValue))
	}

	// Thread-safe accumulation
	r.updateMutex.Lock()
	r.accumulatedUpdateKVs = append(r.accumulatedUpdateKVs, expandedKVs...)
	r.accumulatedUpdateSize += totalSize
	currentSize := r.accumulatedUpdateSize
	// Trigger write if we exceed 230MB minimum, or if we have data and exceeded 256MB maximum
	shouldTriggerWrite := currentSize >= minBatchSize
	r.updateMutex.Unlock()

	// Note: totalWriteOps will be updated when batch is actually written in flushAccumulatedWrites

	// Trigger batch write if we've accumulated enough data
	if shouldTriggerWrite {
		go r.flushAccumulatedWrites()
	}
}

// flushAccumulatedWrites performs a single batch write of accumulated writes (230-256MB)
func (r *PerfRunner) flushAccumulatedWrites() {
	// Get accumulated data and reset
	r.updateMutex.Lock()
	if len(r.accumulatedUpdateKVs) == 0 {
		r.updateMutex.Unlock()
		return
	}

	kvs := make([]KeyValue, len(r.accumulatedUpdateKVs))
	copy(kvs, r.accumulatedUpdateKVs)
	batchSize := r.accumulatedUpdateSize

	// Reset accumulation
	r.accumulatedUpdateKVs = r.accumulatedUpdateKVs[:0]
	r.accumulatedUpdateSize = 0
	r.updateMutex.Unlock()

	// Record batch statistics
	atomic.AddInt64(&r.totalBatchSize, batchSize)
	atomic.AddInt64(&r.totalBatchWrites, 1)
	atomic.AddInt64(&r.totalWriteOps, int64(len(kvs))) // Record actual written KVs

	// Perform actual batch write
	writeStart := time.Now()

	// Create batch with the exact size needed
	batch := r.db.NewBatchWithSize(int(batchSize * 11 / 10)) // Extra 10% for pebble overhead

	for _, kv := range kvs {
		if err := batch.Put(kv.Key, kv.Value); err != nil {
			log.Warn("Failed to add KV to batch", "err", err)
			continue
		}
	}

	// Execute the batch write
	err := batch.Write()
	writeDuration := time.Since(writeStart) // Measure pure write time

	if err != nil {
		log.Error("Failed to write batch", "err", err, "kvCount", len(kvs), "sizeMB", float64(batchSize)/(1024*1024))
	} else {
		sizeMB := float64(batchSize) / (1024 * 1024)
		log.Info("Write batch completed successfully",
			"kvCount", len(kvs),
			"sizeMB", sizeMB,
			"durationMs", writeDuration.Milliseconds(),
			"throughputMB/s", sizeMB*1000/float64(writeDuration.Milliseconds()))
	}

	// Add to write time statistics (pure write time, excluding logging)
	atomic.AddInt64((*int64)(&r.totalWriteTime), int64(writeDuration))
}

// batchWrite function is removed since WriteKVs now use accumulateWrites logic

func (r *PerfRunner) generateKeyWithSamePrefix(originalKey []byte) []byte {
	// Determine prefix length based on key type
	var prefixLen int
	switch {
	case rawdb.IsAccountTrieNode(originalKey):
		// Account trie nodes have variable prefix, keep first 8 bytes as prefix
		prefixLen = min(8, len(originalKey))
	case rawdb.IsStorageTrieNode(originalKey):
		// Storage trie nodes have variable prefix, keep first 8 bytes as prefix
		prefixLen = min(8, len(originalKey))
	case len(originalKey) >= len(rawdb.SnapshotAccountPrefix) &&
		string(originalKey[:len(rawdb.SnapshotAccountPrefix)]) == string(rawdb.SnapshotAccountPrefix):
		// Account snapshot prefix
		prefixLen = len(rawdb.SnapshotAccountPrefix)
	case len(originalKey) >= len(rawdb.SnapshotStoragePrefix) &&
		string(originalKey[:len(rawdb.SnapshotStoragePrefix)]) == string(rawdb.SnapshotStoragePrefix):
		// Storage snapshot prefix
		prefixLen = len(rawdb.SnapshotStoragePrefix)
	default:
		// Default case: keep first 4 bytes as prefix
		prefixLen = min(4, len(originalKey))
	}

	// Create new key with same prefix + random suffix
	newKey := make([]byte, len(originalKey))
	copy(newKey[:prefixLen], originalKey[:prefixLen])

	// Fill the rest with random bytes
	suffixLen := len(originalKey) - prefixLen
	if suffixLen > 0 {
		randomSuffix := make([]byte, suffixLen)
		rand.Read(randomSuffix)
		copy(newKey[prefixLen:], randomSuffix)
	}

	return newKey
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// formatLatency formats latency with appropriate unit (μs, ms, or s)
func formatLatency(latencyMicroseconds float64) string {
	if latencyMicroseconds >= 1000000 { // >= 1 second
		return fmt.Sprintf("%.2f s", latencyMicroseconds/1000000)
	} else if latencyMicroseconds >= 1000 { // >= 1 millisecond
		return fmt.Sprintf("%.2f ms", latencyMicroseconds/1000)
	} else {
		return fmt.Sprintf("%.2f μs", latencyMicroseconds)
	}
}

func (r *PerfRunner) printStat() {
	// Calculate effective TPS (based on actual operation time)
	var mixedReadTPS, snapReadTPS, updateTPS float64
	var mixedReadLatency, snapReadLatency, writeLatency float64

	if r.totalMixedReadTime > 0 {
		mixedReadTPS = float64(r.totalMixedReadOps) * float64(time.Second) / float64(r.totalMixedReadTime)
	}
	if r.totalSnapReadTime > 0 {
		snapReadTPS = float64(r.totalSnapReadOps) * float64(time.Second) / float64(r.totalSnapReadTime)
	}
	if r.totalUpdateTime > 0 {
		updateTPS = float64(r.totalUpdateOps) * float64(time.Second) / float64(r.totalUpdateTime)
	}

	// Calculate average latencies (in microseconds)
	if r.totalMixedReadOps > 0 {
		mixedReadLatency = float64(r.totalMixedReadTime.Microseconds()) / float64(r.totalMixedReadOps)
	}
	if r.totalSnapReadOps > 0 {
		snapReadLatency = float64(r.totalSnapReadTime.Microseconds()) / float64(r.totalSnapReadOps)
	}
	if r.totalBatchWrites > 0 {
		writeLatency = float64(r.totalWriteTime.Microseconds()) / float64(r.totalBatchWrites)
	}

	updateLatency := float64(r.totalUpdateTime.Microseconds()) / float64(r.totalUpdateOps)

	// Calculate batch statistics
	var avgBatchSizeMB float64
	if r.totalBatchWrites > 0 {
		avgBatchSizeMB = float64(r.totalBatchSize) / float64(r.totalBatchWrites) / (1024 * 1024)
	}

	// Calculate accumulated update statistics (thread-safe read)
	r.updateMutex.Lock()
	accumulatedSizeMB := float64(r.accumulatedUpdateSize) / (1024 * 1024)
	accumulatedKVs := len(r.accumulatedUpdateKVs)
	r.updateMutex.Unlock()

	// Calculate average KVs per batch
	var avgKVsPerBatch float64
	if r.totalBatchWrites > 0 {
		avgKVsPerBatch = float64(r.totalWriteOps) / float64(r.totalBatchWrites)
	}

	fmt.Printf(
		"[%s] Perf In Progress - block height=%d\n"+
			"  Mixed Read TPS: %.2f, Latency: %.2f μs | Snap Read TPS: %.2f, Latency: %.2f μs\n"+
			"  Update TPS: %.2f, Latency: %.2f μs  | Write Batch: Latency: %s, Avg KVs: %.0f, Avg Size: %.1f MB, Count: %d\n"+
			"  Accumulated Updates: %d KVs, %.2f MB (target: 230-256MB)\n",
		time.Now().Format(time.RFC3339),
		r.blockHeight,
		mixedReadTPS, mixedReadLatency, snapReadTPS, snapReadLatency,
		updateTPS, updateLatency, formatLatency(writeLatency), avgKVsPerBatch, avgBatchSizeMB, r.totalBatchWrites,
		accumulatedKVs, accumulatedSizeMB,
	)

	// Update last counters
	r.lastReadOps = r.totalReadOps
	r.lastWriteOps = r.totalWriteOps
	r.lastUpdateOps = r.totalUpdateOps
	r.lastBatchCount = r.totalBatchCount
	r.lastUpdateKVs = r.totalUpdateKVs
	r.lastUpdateSize = r.totalUpdateSize
	r.lastStatTime = time.Now()
}

func (r *PerfRunner) printAVGStat(startTime time.Time) {
	elapsed := time.Since(startTime)

	var avgMixedReadLatency, avgSnapReadLatency, avgWriteLatency, avgUpdateLatency float64

	if r.totalMixedReadOps > 0 {
		avgMixedReadLatency = float64(r.totalMixedReadTime.Microseconds()) / float64(r.totalMixedReadOps)
	}
	if r.totalSnapReadOps > 0 {
		avgSnapReadLatency = float64(r.totalSnapReadTime.Microseconds()) / float64(r.totalSnapReadOps)
	}
	if r.totalBatchWrites > 0 {
		avgWriteLatency = float64(r.totalWriteTime.Microseconds()) / float64(r.totalBatchWrites)
	}
	if r.totalUpdateOps > 0 {
		avgUpdateLatency = float64(r.totalUpdateTime.Microseconds()) / float64(r.totalUpdateOps)
	}

	// Calculate effective TPS (based on actual operation time)
	var mixedReadTPS, snapReadTPS, updateTPS float64
	if r.totalMixedReadTime > 0 {
		mixedReadTPS = float64(r.totalMixedReadOps) * float64(time.Second) / float64(r.totalMixedReadTime)
	}
	if r.totalSnapReadTime > 0 {
		snapReadTPS = float64(r.totalSnapReadOps) * float64(time.Second) / float64(r.totalSnapReadTime)
	}
	if r.totalUpdateTime > 0 {
		updateTPS = float64(r.totalUpdateOps) * float64(time.Second) / float64(r.totalUpdateTime)
	}

	// Calculate batch statistics
	var avgBatchSizeMB float64
	if r.totalBatchWrites > 0 {
		avgBatchSizeMB = float64(r.totalBatchSize) / float64(r.totalBatchWrites) / (1024 * 1024)
	}

	// Calculate total update statistics (including both individual updates and batch writes)

	// Check for remaining accumulated data
	r.updateMutex.Lock()
	remainingAccumulatedMB := float64(r.accumulatedUpdateSize) / (1024 * 1024)
	remainingKVs := len(r.accumulatedUpdateKVs)
	r.updateMutex.Unlock()

	// Calculate average KVs per batch for final summary
	var avgKVsPerBatch float64
	if r.totalBatchWrites > 0 {
		avgKVsPerBatch = float64(r.totalWriteOps) / float64(r.totalBatchWrites)
	}

	fmt.Printf(
		"=== Average Performance Metrics ===\n"+
			"Elapsed: %v, Block Height: %d\n"+
			"Mixed Read  - Avg Latency: %.2f μs, TPS: %.2f, Total Ops: %d\n"+
			"Snap Read   - Avg Latency: %.2f μs, TPS: %.2f, Total Ops: %d\n"+
			"Update      - Avg Latency: %.2f μs, TPS: %.2f, Total KVs: %d\n"+
			"Write Batch - Avg Latency: %s, Batch Count: %d, Avg KVs: %.0f, Avg Size: %.1f MB\n",
		elapsed,
		r.blockHeight,
		avgMixedReadLatency, mixedReadTPS, r.totalMixedReadOps,
		avgSnapReadLatency, snapReadTPS, r.totalSnapReadOps,
		avgUpdateLatency, updateTPS, r.totalUpdateKVs,
		formatLatency(avgWriteLatency), r.totalBatchWrites, avgKVsPerBatch, avgBatchSizeMB,
	)

	if remainingKVs > 0 {
		fmt.Printf("Remaining Accumulated Updates: %d KVs, %.2f MB (will be flushed on shutdown)\n",
			remainingKVs, remainingAccumulatedMB)
	}
	fmt.Printf("===================================\n")
}

// printHashSummary prints hash calculation summary statistics
func (r *PerfRunner) printHashSummary() {
	if r.totalHashOps > 0 {
		avgHashLatency := float64(r.totalHashTime.Microseconds()) / float64(r.totalHashOps)
		avgCommitLatency := float64(r.totalCommitTime.Microseconds()) / float64(r.totalHashOps)

		fmt.Printf("=== Hash Calculation Summary ===\n")
		fmt.Printf("Total Hash Operations: %d\n", r.totalHashOps)
		fmt.Printf("Total Hash Time: %v\n", r.totalHashTime)
		fmt.Printf("Total Commit Time: %v\n", r.totalCommitTime)

		// Format hash latency with appropriate units
		if avgHashLatency >= 1000000 { // >= 1 second
			fmt.Printf("Average Hash Latency: %.2f s\n", avgHashLatency/1000000)
		} else if avgHashLatency >= 1000 { // >= 1 millisecond
			fmt.Printf("Average Hash Latency: %.2f ms\n", avgHashLatency/1000)
		} else {
			fmt.Printf("Average Hash Latency: %.2f μs\n", avgHashLatency)
		}

		// Format commit latency with appropriate units
		if avgCommitLatency >= 1000000 { // >= 1 second
			fmt.Printf("Average Commit Latency: %.2f s\n", avgCommitLatency/1000000)
		} else if avgCommitLatency >= 1000 { // >= 1 millisecond
			fmt.Printf("Average Commit Latency: %.2f ms\n", avgCommitLatency/1000)
		} else {
			fmt.Printf("Average Commit Latency: %.2f μs\n", avgCommitLatency)
		}

		fmt.Printf("===============================\n")
	} else {
		fmt.Printf("=== Hash Calculation Summary ===\n")
		fmt.Printf("No hash operations performed\n")
		fmt.Printf("===============================\n")
	}
}

// calculateHashRoot performs trie hash calculation for performance measurement
func (r *PerfRunner) calculateHashRoot() {
	// Initialize trie components only once
	if r.trieDB == nil || r.theTrie == nil {
		if err := r.initializeTrie(); err != nil {
			log.Warn("Failed to initialize trie", "err", err)
			return
		}
		// For the first call, don't count initialization time in hash statistics
		// Just update the operation count
		atomic.AddInt64(&r.totalHashOps, 1)
		log.Info("Trie initialization completed for first hash calculation")
		return
	}

	// Execute commit operation
	commitStart := time.Now()
	newRoot, nodes := r.theTrie.Commit(false)
	commitDuration := time.Since(commitStart)

	// Measure hash calculation after commit
	hashAfterStart := time.Now()
	computedHash := r.theTrie.Hash()
	hashAfterDuration := time.Since(hashAfterStart)

	// Total time for actual hash operations (only hash after commit)
	actualHashTime := hashAfterDuration

	// Calculate updated nodes count, prevent nil pointer dereference
	var nodesUpdated int
	if nodes != nil && nodes.Nodes != nil {
		nodesUpdated = len(nodes.Nodes)
	}

	// Record results with detailed timing
	log.Info("Hash calculation with real blockchain data",
		"computed_hash", computedHash.Hex(),
		"new_root", newRoot.Hex(),
		"commit_time_μs", commitDuration.Microseconds(),
		"hash_after_μs", hashAfterDuration.Microseconds(),
		"commit_time_ns", commitDuration.Nanoseconds(),
		"hash_after_ns", hashAfterDuration.Nanoseconds(),
		"actual_hash_time_μs", actualHashTime.Microseconds(),
		"nodes_updated", nodesUpdated)

	// Update statistics using only the actual hash calculation time
	atomic.AddInt64(&r.totalHashOps, 1)
	atomic.AddInt64((*int64)(&r.totalHashTime), int64(actualHashTime))
	atomic.AddInt64((*int64)(&r.totalCommitTime), int64(commitDuration))
}

// initializeTrie initializes the trie components once
func (r *PerfRunner) initializeTrie() error {
	// Use pre-created chainDB
	db := r.chainDB

	var (
		blockNumber  uint64
		trieRootHash common.Hash
	)

	// Get latest block header - same as inspect-trie logic
	headerHash := rawdb.ReadHeadHeaderHash(db)
	if headerHash != (common.Hash{}) {
		if headerNum := rawdb.ReadHeaderNumber(db, headerHash); headerNum != nil {
			blockNumber = *headerNum

			if blockNumber != math.MaxUint64 {
				headerBlockHash := rawdb.ReadCanonicalHash(db, blockNumber)
				if headerBlockHash == (common.Hash{}) {
					return fmt.Errorf("ReadHeadBlockHash empty hash")
				}
				blockHeader := rawdb.ReadHeader(db, headerBlockHash, blockNumber)
				if blockHeader != nil {
					trieRootHash = blockHeader.Root
				}
			}
		}
	} else {
		return fmt.Errorf("No head header hash found in database")
	}

	if trieRootHash == (common.Hash{}) {
		return fmt.Errorf("Empty root hash")
	}

	log.Info("Initializing trie for hash calculations",
		"root", trieRootHash.Hex(),
		"block_number", blockNumber)

	// Detect database scheme and create corresponding triedb config
	dbScheme := rawdb.ReadStateScheme(db)
	var config *triedb.Config
	if dbScheme == rawdb.PathScheme {
		config = &triedb.Config{
			PathDB: utils.PathDBConfigAddJournalFilePath(r.stack, pathdb.ReadOnly),
			Cache:  0,
		}
	} else if dbScheme == rawdb.HashScheme {
		config = triedb.HashDefaults
	}

	// Create triedb and trie (only once)
	r.trieDB = triedb.NewDatabase(db, config)

	theTrie, err := trie.New(trie.TrieID(trieRootHash), r.trieDB)
	if err != nil {
		return fmt.Errorf("failed to create trie: %v", err)
	}
	r.theTrie = theTrie

	log.Info("Trie initialized successfully", "db_scheme", dbScheme)
	return nil
}

// makeConfigNode creates a simplified node configuration for database access
func makeConfigNode(ctx *cli.Context, benchDBPath string) (*node.Node, error) {
	// Create default configuration
	cfg := node.DefaultConfig
	cfg.Name = ""
	cfg.DataDir = benchDBPath // Use bench-db path as data directory

	// Apply any CLI context flags to node config if available
	// This will properly handle other node flags
	if ctx != nil {
		utils.SetNodeConfig(ctx, &cfg)
		// Override DataDir with benchDBPath to ensure we use the correct path
		cfg.DataDir = benchDBPath
	}

	// Create the node
	stack, err := node.New(&cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create protocol stack: %v", err)
	}

	return stack, nil
}
