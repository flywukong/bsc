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
	TestCaseDir string
	BenchDBPath string
	BatchSize   uint64
	ReadRatio   float64
	WriteRatio  float64
	UpdateRatio float64
	NumThreads  int
	RuntimeDur  time.Duration
	MetricsAddr string
	MetricsPort int
	CacheSize   int // Database cache size in MB
	Handles     int // Number of file descriptor handles
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
	ReadKVs   []KeyValue
	UpdateKVs []KeyValue
	WriteKVs  []KeyValue
}

type PerfRunner struct {
	dataSet  *DataSet
	db       *pebble.Database
	config   PerfConfig
	taskChan chan *Task
	ctx      *cli.Context // CLI context for database operations

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

	// Hash calculation statistics
	totalHashOps  int64
	totalHashTime time.Duration
	trieDir       string // Directory for trie operations

	// For interval TPS calculation
	lastReadOps    int64
	lastWriteOps   int64
	lastUpdateOps  int64
	lastBatchCount int64
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
		case len(key) >= len(rawdb.SnapshotAccountPrefix) &&
			string(key[:len(rawdb.SnapshotAccountPrefix)]) == string(rawdb.SnapshotAccountPrefix):
			kv.Type = AccountSnaps
			dataSet.AccountSnaps = append(dataSet.AccountSnaps, kv)
		case len(key) >= len(rawdb.SnapshotStoragePrefix) &&
			string(key[:len(rawdb.SnapshotStoragePrefix)]) == string(rawdb.SnapshotStoragePrefix):
			kv.Type = StorageSnaps
			dataSet.StorageSnaps = append(dataSet.StorageSnaps, kv)
		}
	}

	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("iterator error: %v", err)
	}

	return dataSet, nil
}

func NewPerfRunner(dataSet *DataSet, db *pebble.Database, config PerfConfig, ctx *cli.Context) *PerfRunner {
	return &PerfRunner{
		dataSet:      dataSet,
		db:           db,
		config:       config,
		taskChan:     make(chan *Task, 10),
		ctx:          ctx,
		lastStatTime: time.Now(),
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

	// Select 1/4 from each data type to form the batch
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

	task.ReadKVs = allKVs[:readCount]
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
	var wg sync.WaitGroup

	// Process read operations concurrently
	if len(task.ReadKVs) > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			readStart := time.Now()
			var readWG sync.WaitGroup
			r.processReadsParallel(task.ReadKVs, &readWG)
			readWG.Wait()
			atomic.AddInt64((*int64)(&r.totalReadTime), int64(time.Since(readStart)))
		}()
	}

	// Process update operations concurrently
	if len(task.UpdateKVs) > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			updateStart := time.Now()
			var updateWG sync.WaitGroup
			r.processUpdatesParallel(task.UpdateKVs, &updateWG)
			updateWG.Wait()
			atomic.AddInt64((*int64)(&r.totalUpdateTime), int64(time.Since(updateStart)))
		}()
	}

	// Process write operations concurrently (but still single-threaded batch write)
	if len(task.WriteKVs) > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			writeStart := time.Now()
			r.batchWrite(task.WriteKVs)
			atomic.AddInt64((*int64)(&r.totalWriteTime), int64(time.Since(writeStart)))
		}()
	}

	// Wait for all operations to complete
	wg.Wait()
}

func (r *PerfRunner) processReadsParallel(readKVs []KeyValue, wg *sync.WaitGroup) {
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

			atomic.AddInt64(&r.totalReadOps, localReadOps)
		}(readKVs[start:end])
	}
}

func (r *PerfRunner) processUpdatesParallel(updateKVs []KeyValue, wg *sync.WaitGroup) {
	numThreads := r.config.NumThreads
	if numThreads <= 0 {
		numThreads = 1
	}

	chunkSize := len(updateKVs) / numThreads
	if chunkSize == 0 {
		chunkSize = 1
	}

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

			for _, kv := range kvs {
				// Modify value slightly for update
				newValue := append(kv.Value, byte(mathrand.Intn(256)))
				err := r.db.Put(kv.Key, newValue)
				if err != nil {
					log.Warn("Failed to update key", "err", err)
				}
				localUpdateOps++
			}

			atomic.AddInt64(&r.totalUpdateOps, localUpdateOps)
		}(updateKVs[start:end])
	}
}

// batchWrite performs batch write operations with size control (230MB-256MB per batch)
func (r *PerfRunner) batchWrite(kvs []KeyValue) {
	const (
		targetBatchSize = 256 * 1024 * 1024 // 256MB per batch
		minBatchSize    = 230 * 1024 * 1024 // 230MB minimum
	)

	if len(kvs) == 0 {
		return
	}

	// Create first batch with target size plus 10% extra for pebble internal stuff
	batch := r.db.NewBatchWithSize(targetBatchSize * 11 / 10)
	currentBatchSize := 0
	batchEntryCount := 0
	totalProcessed := 0
	batchNumber := 1

	for i, kv := range kvs {
		// Generate new key with same prefix
		newKey := r.generateKeyWithSamePrefix(kv.Key)
		// Also modify value to make it different from original
		newValue := append(kv.Value, byte(mathrand.Intn(256)))

		// Estimate entry size before adding
		entrySize := len(newKey) + len(newValue) + 64 // 64 bytes overhead estimate

		// Check if adding this entry would exceed the batch size limit
		wouldExceedLimit := (currentBatchSize + entrySize) > targetBatchSize
		isLastEntry := (i == len(kvs)-1)

		// If this entry would exceed limit and we have some entries, commit current batch first
		if wouldExceedLimit && batchEntryCount > 0 && currentBatchSize >= minBatchSize {
			// Commit current batch
			size := batch.ValueSize()
			if err := batch.Write(); err != nil {
				log.Warn("Failed to write batch", "err", err, "size", size, "entries", batchEntryCount, "batch", batchNumber)
			} else {
				r.totalWriteOps += int64(batchEntryCount)
				r.totalBatchCount++ // Increment batch counter
				log.Debug("Batch write completed", "size", size, "entries", batchEntryCount, "batch", batchNumber,
					"processed", totalProcessed, "total", len(kvs))
			}

			// Start new batch
			batchNumber++
			batch = r.db.NewBatchWithSize(targetBatchSize * 11 / 10)
			currentBatchSize = 0
			batchEntryCount = 0
		}

		// Add entry to current batch with both new key and new value
		err := batch.Put(newKey, newValue)
		if err != nil {
			log.Warn("Failed to add key to batch", "err", err)
			continue
		}

		currentBatchSize += entrySize
		batchEntryCount++
		totalProcessed++

		// Commit if this is the last entry and we have entries in batch
		if isLastEntry && batchEntryCount > 0 {
			size := batch.ValueSize()
			if err := batch.Write(); err != nil {
				log.Warn("Failed to write final batch", "err", err, "size", size, "entries", batchEntryCount, "batch", batchNumber)
			} else {
				r.totalWriteOps += int64(batchEntryCount)
				r.totalBatchCount++ // Increment batch counter
				log.Debug("Final batch write completed", "size", size, "entries", batchEntryCount, "batch", batchNumber,
					"processed", totalProcessed, "total", len(kvs))
			}
		}
	}
}

// generateKeyWithSamePrefix generates a new key keeping the same prefix as the original key
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

func (r *PerfRunner) printStat() {
	// Calculate effective TPS (based on actual operation time)
	var readTPS, writeTPS, updateTPS float64

	if r.totalReadTime > 0 {
		readTPS = float64(r.totalReadOps) * float64(time.Second) / float64(r.totalReadTime)
	}
	if r.totalWriteTime > 0 {
		writeTPS = float64(r.totalWriteOps) * float64(time.Second) / float64(r.totalWriteTime)
	}
	if r.totalUpdateTime > 0 {
		updateTPS = float64(r.totalUpdateOps) * float64(time.Second) / float64(r.totalUpdateTime)
	}

	fmt.Printf(
		"[%s] Perf In Progress - block height=%d\n"+
			"  Read TPS: %.2f, Write TPS: %.2f, Update TPS: %.2f\n",
		time.Now().Format(time.RFC3339),
		r.blockHeight,
		readTPS, writeTPS, updateTPS,
	)

	// Update last counters
	r.lastReadOps = r.totalReadOps
	r.lastWriteOps = r.totalWriteOps
	r.lastUpdateOps = r.totalUpdateOps
	r.lastBatchCount = r.totalBatchCount
	r.lastStatTime = time.Now()
}

func (r *PerfRunner) printAVGStat(startTime time.Time) {
	elapsed := time.Since(startTime)

	var avgReadLatency, avgWriteLatency, avgUpdateLatency float64

	if r.totalReadOps > 0 {
		avgReadLatency = float64(r.totalReadTime.Microseconds()) / float64(r.totalReadOps)
	}
	if r.totalWriteOps > 0 {
		avgWriteLatency = float64(r.totalWriteTime.Microseconds()) / float64(r.totalWriteOps)
	}
	if r.totalUpdateOps > 0 {
		avgUpdateLatency = float64(r.totalUpdateTime.Microseconds()) / float64(r.totalUpdateOps)
	}

	// Calculate effective TPS (based on actual operation time)
	var readTPS, writeTPS, updateTPS float64
	if r.totalReadTime > 0 {
		readTPS = float64(r.totalReadOps) * float64(time.Second) / float64(r.totalReadTime)
	}
	if r.totalWriteTime > 0 {
		writeTPS = float64(r.totalWriteOps) * float64(time.Second) / float64(r.totalWriteTime)
	}
	if r.totalUpdateTime > 0 {
		updateTPS = float64(r.totalUpdateOps) * float64(time.Second) / float64(r.totalUpdateTime)
	}

	fmt.Printf(
		"=== Average Performance Metrics ===\n"+
			"Elapsed: %v, Block Height: %d\n"+
			"Read  - Avg Latency: %.2f μs, TPS: %.2f, Total Ops: %d\n"+
			"Write - Avg Latency: %.2f μs, TPS: %.2f, Total Ops: %d\n"+
			"Update- Avg Latency: %.2f μs, TPS: %.2f, Total Ops: %d\n"+
			"===================================\n",
		elapsed,
		r.blockHeight,
		avgReadLatency, readTPS, r.totalReadOps,
		avgWriteLatency, writeTPS, r.totalWriteOps,
		avgUpdateLatency, updateTPS, r.totalUpdateOps,
	)
}

// printHashSummary prints hash calculation summary statistics
func (r *PerfRunner) printHashSummary() {
	if r.totalHashOps > 0 {
		avgHashLatency := float64(r.totalHashTime.Microseconds()) / float64(r.totalHashOps)
		fmt.Printf("=== Hash Calculation Summary ===\n")
		fmt.Printf("Total Hash Operations: %d\n", r.totalHashOps)
		fmt.Printf("Total Hash Time: %v\n", r.totalHashTime)
		fmt.Printf("Average Hash Latency: %.2f μs\n", avgHashLatency)
		fmt.Printf("===============================\n")
	} else {
		fmt.Printf("=== Hash Calculation Summary ===\n")
		fmt.Printf("No hash operations performed\n")
		fmt.Printf("===============================\n")
	}
}

// calculateHashRoot performs trie hash calculation for performance measurement
func (r *PerfRunner) calculateHashRoot() {
	totalStart := time.Now()

	// Use inspect-trie approach for database initialization
	stack, err := makeConfigNode(r.ctx, r.config.BenchDBPath)
	if err != nil {
		log.Warn("Failed to create config node", "err", err)
		return
	}
	defer stack.Close()

	db := utils.MakeChainDatabase(r.ctx, stack, true, false)
	defer db.Close()

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
					log.Error("ReadHeadBlockHash empty hash")
					return
				}
				blockHeader := rawdb.ReadHeader(db, headerBlockHash, blockNumber)
				if blockHeader != nil {
					trieRootHash = blockHeader.Root
				}
			}
		}
	} else {
		log.Warn("No head header hash found in database")
		return
	}

	if trieRootHash == (common.Hash{}) {
		log.Error("Empty root hash")
		return
	}

	fmt.Printf("ReadBlockHeader, root: %v, blocknum: %v\n", trieRootHash, blockNumber)

	// Detect database scheme and create corresponding triedb config
	dbScheme := rawdb.ReadStateScheme(db)
	var config *triedb.Config
	if dbScheme == rawdb.PathScheme {
		config = &triedb.Config{
			PathDB: utils.PathDBConfigAddJournalFilePath(stack, pathdb.ReadOnly),
			Cache:  0,
		}
	} else if dbScheme == rawdb.HashScheme {
		config = triedb.HashDefaults
	}

	// Create triedb and trie
	trieDB := triedb.NewDatabase(db, config)
	defer trieDB.Close()

	theTrie, err := trie.New(trie.TrieID(trieRootHash), trieDB)
	if err != nil {
		log.Warn("Failed to create trie", "err", err, "root", trieRootHash.Hex())
		return
	}

	// 执行hash计算性能测试
	commitStart := time.Now()
	newRoot, nodes := theTrie.Commit(false)
	commitDuration := time.Since(commitStart)

	hashStart := time.Now()
	computedHash := theTrie.Hash()
	hashDuration := time.Since(hashStart)

	totalDuration := time.Since(totalStart)

	// 计算比值
	var commitVsHashRatio float64
	if hashDuration.Nanoseconds() > 0 {
		commitVsHashRatio = float64(commitDuration.Nanoseconds()) / float64(hashDuration.Nanoseconds())
	}

	// 记录结果
	log.Info("Hash calculation with real blockchain data",
		"original_root", trieRootHash.Hex(),
		"computed_hash", computedHash.Hex(),
		"new_root", newRoot.Hex(),
		"block_number", blockNumber,
		"db_scheme", dbScheme,
		"commit_time_μs", commitDuration.Microseconds(),
		"hash_time_μs", hashDuration.Microseconds(),
		"total_time_μs", totalDuration.Microseconds(),
		"commit_vs_hash_ratio", fmt.Sprintf("%.2f", commitVsHashRatio),
		"nodes_updated", len(nodes.Nodes))

	// 更新统计
	atomic.AddInt64(&r.totalHashOps, 1)
	atomic.AddInt64((*int64)(&r.totalHashTime), int64(totalDuration))
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
