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
	"encoding/binary"
	"fmt"
	"math"
	mathrand "math/rand"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/ethdb/pebble"
	"github.com/ethereum/go-ethereum/ethdb/shardingdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/node"
	"github.com/urfave/cli/v2"
)

const version = "1.0.0"

// Percentile histogram config: linear buckets in microseconds
const latencyHistUSMax = 5000 // 0..5000us in 1us steps, >5000us in overflow

type PerfConfig struct {
	TestCaseDir string
	BenchDBPath string
	BatchSize   uint64
	ReadRatio   float64
	WriteRatio  float64
	UpdateRatio float64
	NumThreads  int
	RuntimeDur  time.Duration
	WarmupDur   time.Duration // Warmup duration before measuring
	Seed        int64         // Random seed (0 for random)
	MetricsAddr string
	MetricsPort int
	CacheSize   int  // Database cache size in MB
	Handles     int  // Number of file descriptor handles
	ShardingDB  bool // Enable sharding database mode
}

type DataType int

const (
	AccountTries DataType = iota
	StorageTries
)

type KeyValue struct {
	Key   []byte
	Value []byte
	Type  DataType
}

type DataSet struct {
	AccountTries []KeyValue
	StorageTries []KeyValue
}

type Task struct {
	ReadKVs   []KeyValue // Trie reads (AccountTries + StorageTries)
	UpdateKVs []KeyValue
	WriteKVs  []KeyValue
}

type PerfRunner struct {
	dataSet  *DataSet
	db       ethdb.Database // Benchmark database for read/write/update operations
	config   PerfConfig
	taskChan chan *Task
	ctx      *cli.Context // CLI context for database operations
	stack    *node.Node   // Node stack for database

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

	// Read statistics for trie types
	totalTrieReadOps  int64
	totalTrieReadTime time.Duration
	// Histograms for percentiles (linear microsecond buckets up to threshold)
	trieLatencyHist [latencyHistUSMax + 2]int64 // 0..max, overflow at last index
	minTrieReadTime int64                       // stored as nanoseconds for atomic operations
	maxTrieReadTime int64                       // stored as nanoseconds for atomic operations

	// Update batch statistics - for 230-256MB accumulated batch write
	totalUpdateKVs        int64      // Total number of KVs in update batches
	totalUpdateSize       int64      // Total size of update batches in bytes
	accumulatedUpdateKVs  []KeyValue // In-memory accumulation for 230-256MB batch
	accumulatedUpdateSize int64      // Current accumulated size in bytes
	updateMutex           sync.Mutex // Protect accumulated update data
	minUpdateTime         int64      // stored as nanoseconds for atomic operations
	maxUpdateTime         int64      // stored as nanoseconds for atomic operations
	updateLatencyHist     [latencyHistUSMax + 2]int64

	// Write batch statistics
	totalBatchSize   int64 // Total size of all write batches
	totalBatchWrites int64 // Total number of write batches executed
	minWriteTime     int64 // stored as nanoseconds for atomic operations
	maxWriteTime     int64 // stored as nanoseconds for atomic operations

	// For interval TPS calculation
	lastReadOps    int64
	lastWriteOps   int64
	lastUpdateOps  int64
	lastBatchCount int64
	lastUpdateKVs  int64 // Last update KV count for interval calculation
	lastUpdateSize int64 // Last update size for interval calculation
}

func main() {
	// 使用加密安全的随机数初始化随机种子
	var seed int64
	if err := binary.Read(rand.Reader, binary.BigEndian, &seed); err != nil {
		// 如果加密随机数失败，使用时间作为后备
		seed = time.Now().UnixNano()
	}
	mathrand.Seed(seed)

	var config PerfConfig

	app := &cli.App{
		Name:    "state-perf",
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
					&cli.Int64Flag{
						Name:        "seed",
						Usage:       "Random seed (0 for random)",
						Value:       0,
						Destination: &config.Seed,
					},
					&cli.DurationFlag{
						Name:        "warmup",
						Usage:       "Warmup duration before measuring (e.g. 10m, 5m, 30s)",
						Value:       10 * time.Minute,
						Destination: &config.WarmupDur,
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
					&cli.BoolFlag{
						Name:        "sharding-db",
						Usage:       "Enable sharding database mode",
						Destination: &config.ShardingDB,
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
		"runtime", config.RuntimeDur,
		"shardingDB", config.ShardingDB)

	// Load data-set from test-case directory
	log.Info("Loading data-set from test-case directory", "path", config.TestCaseDir)
	dataSet, err := loadDataSet(config.TestCaseDir)
	if err != nil {
		return fmt.Errorf("failed to load data-set: %v", err)
	}

	log.Info("Data-set loaded successfully",
		"accountTries", len(dataSet.AccountTries),
		"storageTries", len(dataSet.StorageTries),
		"total",
		len(dataSet.AccountTries)+len(dataSet.StorageTries))

	// Create node stack for database operations
	stack, err := makeConfigNode(c, config.BenchDBPath)
	if err != nil {
		return fmt.Errorf("failed to create node stack: %v", err)
	}
	defer stack.Close()

	var benchDB ethdb.Database

	// Configure sharding database if enabled
	if config.ShardingDB {
		log.Info("Creating sharding database for benchmark operations")

		// Create sharding database configuration
		shardingConfig := &shardingdb.Config{
			EnableSharding: true,
			DBType:         shardingdb.DBTypePebble,
			DBPath:         config.BenchDBPath,
			Namespace:      "",
			ShardNum:       8, // Use 8 shards for testing
			Shards: []shardingdb.ShardConfig{
				{
					DBPath:  config.BenchDBPath + "/shard0000",
					Indexes: "0",
				},
				{
					DBPath:  config.BenchDBPath + "/shard0001",
					Indexes: "1",
				},
				{
					DBPath:  config.BenchDBPath + "/shard0002",
					Indexes: "2",
				},
				{
					DBPath:  config.BenchDBPath + "/shard0003",
					Indexes: "3",
				},
				{
					DBPath:  config.BenchDBPath + "/shard0004",
					Indexes: "4",
				},
				{
					DBPath:  config.BenchDBPath + "/shard0005",
					Indexes: "5",
				},
				{
					DBPath:  config.BenchDBPath + "/shard0006",
					Indexes: "6",
				},
				{
					DBPath:  config.BenchDBPath + "/shard0007",
					Indexes: "7",
				},
			},
		}

		// Create sharding database
		shardDB, err := shardingdb.New(shardingConfig, config.CacheSize, config.Handles, false, rawdb.ShardIndexInTrieDB)
		if err != nil {
			return fmt.Errorf("failed to create sharding database: %v", err)
		}

		// Use rawdb.NewDatabase to wrap sharding database
		benchDB = rawdb.NewDatabase(shardDB)
		log.Info("Sharding database created successfully", "shards", shardingConfig.ShardNum)
	} else {
		// Create benchmark database using OpenDatabaseWithFreezer
		benchDB, err = stack.OpenDatabaseWithFreezer("chaindata", config.CacheSize, config.Handles, "", "", false, false)
		if err != nil {
			return fmt.Errorf("failed to create benchmark database: %v", err)
		}
	}
	defer benchDB.Close()

	// Create and start performance runner
	runner := NewPerfRunner(dataSet, benchDB, *config, c, stack)
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
	log.Info("Loading test data with prefix-based scanning", "testCaseDir", testCaseDir)

	// Open test-case pebble database using simple pebble.New
	db, err := pebble.New(testCaseDir, 4096, 32766, "", true) // readonly
	if err != nil {
		return nil, fmt.Errorf("failed to open test-case database: %v", err)
	}
	defer db.Close()

	dataSet := &DataSet{}

	// Limit to 500 million keys
	const maxKeys = 500_000_000
	totalScanned := 0

	// Load account trie nodes using prefix scan
	log.Info("Scanning account trie nodes", "prefix", "TrieNodeAccountPrefix", "maxKeys", maxKeys)
	accountIter := db.NewIterator(rawdb.TrieNodeAccountPrefix, nil)
	defer accountIter.Release()

	accountCount := 0
	for accountIter.Next() && totalScanned < maxKeys {
		key := make([]byte, len(accountIter.Key()))
		value := make([]byte, len(accountIter.Value()))
		copy(key, accountIter.Key())
		copy(value, accountIter.Value())

		totalScanned++

		// Verify it's actually an account trie node
		if rawdb.IsAccountTrieNode(key) {
			kv := KeyValue{
				Key:   key,
				Value: value,
				Type:  AccountTries,
			}
			dataSet.AccountTries = append(dataSet.AccountTries, kv)
			accountCount++
		}

		if totalScanned%1000000 == 0 {
			log.Info("Account trie scan progress",
				"scanned", totalScanned,
				"loaded", accountCount,
				"progress", fmt.Sprintf("%.1f%%", float64(totalScanned)*100/float64(maxKeys)))
		}

		// Check if we've reached the limit
		if totalScanned >= maxKeys {
			log.Warn("Reached maximum key limit during account trie scan",
				"limit", maxKeys,
				"accountTriesLoaded", accountCount)
			break
		}
	}

	if err := accountIter.Error(); err != nil {
		return nil, fmt.Errorf("account trie iterator error: %v", err)
	}

	log.Info("Account trie scan completed",
		"scanned", totalScanned,
		"loaded", accountCount,
		"reachedLimit", totalScanned >= maxKeys)

	// Load storage trie nodes using prefix scan (only if we haven't reached the limit)
	if totalScanned < maxKeys {
		log.Info("Scanning storage trie nodes",
			"prefix", "TrieNodeStoragePrefix",
			"remaining", maxKeys-totalScanned)
		storageIter := db.NewIterator(rawdb.TrieNodeStoragePrefix, nil)
		defer storageIter.Release()

		storageCount := 0
		for storageIter.Next() && totalScanned < maxKeys {
			key := make([]byte, len(storageIter.Key()))
			value := make([]byte, len(storageIter.Value()))
			copy(key, storageIter.Key())
			copy(value, storageIter.Value())

			totalScanned++

			// Verify it's actually a storage trie node
			if rawdb.IsStorageTrieNode(key) {
				kv := KeyValue{
					Key:   key,
					Value: value,
					Type:  StorageTries,
				}
				dataSet.StorageTries = append(dataSet.StorageTries, kv)
				storageCount++
			}

			if totalScanned%1000000 == 0 {
				log.Info("Storage trie scan progress",
					"scanned", totalScanned,
					"loaded", storageCount,
					"progress", fmt.Sprintf("%.1f%%", float64(totalScanned)*100/float64(maxKeys)))
			}

			// Check if we've reached the limit
			if totalScanned >= maxKeys {
				log.Warn("Reached maximum key limit during storage trie scan",
					"limit", maxKeys,
					"storageTriesLoaded", storageCount)
				break
			}
		}

		if err := storageIter.Error(); err != nil {
			return nil, fmt.Errorf("storage trie iterator error: %v", err)
		}

		log.Info("Storage trie scan completed",
			"scanned", totalScanned,
			"loaded", storageCount,
			"reachedLimit", totalScanned >= maxKeys)
	} else {
		log.Info("Skipping storage trie scan - already reached key limit")
	}

	// Log final data set statistics
	totalNodes := len(dataSet.AccountTries) + len(dataSet.StorageTries)
	log.Info("Test data loading completed",
		"totalScanned", totalScanned,
		"maxKeys", maxKeys,
		"accountTries", len(dataSet.AccountTries),
		"storageTries", len(dataSet.StorageTries),
		"totalTrieNodesLoaded", totalNodes,
		"limitReached", totalScanned >= maxKeys)

	fmt.Printf("Data set loaded successfully:\n")
	fmt.Printf("  Total Scanned: %d (limit: %d)\n", totalScanned, maxKeys)
	fmt.Printf("  Account Tries: %d\n", len(dataSet.AccountTries))
	fmt.Printf("  Storage Tries: %d\n", len(dataSet.StorageTries))
	fmt.Printf("  Total Trie nodes: %d\n", totalNodes)
	if totalScanned >= maxKeys {
		fmt.Printf("  *** LIMIT REACHED - stopped at %d keys ***\n", maxKeys)
	}

	return dataSet, nil
}

func NewPerfRunner(dataSet *DataSet, db ethdb.Database, config PerfConfig, ctx *cli.Context, stack *node.Node) *PerfRunner {
	runner := &PerfRunner{
		dataSet:      dataSet,
		db:           db,
		config:       config,
		taskChan:     make(chan *Task, 10),
		ctx:          ctx,
		stack:        stack,
		lastStatTime: time.Now(),
	}

	return runner
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

	// Use per-run RNG with optional fixed seed
	if r.config.Seed != 0 {
		mathrand.Seed(r.config.Seed)
		log.Info("Using fixed random seed", "seed", r.config.Seed)
	}

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

	// Calculate counts by ratios for each operation type (uniform sampling across trie datasets)
	total := int(r.config.BatchSize)
	readCount := int(float64(total) * r.config.ReadRatio)
	updateCount := int(float64(total) * r.config.UpdateRatio)
	writeCount := total - readCount - updateCount

	// Read operations: mix of AccountTries and StorageTries
	// 60% StorageTries, 40% AccountTries
	readKVs := make([]KeyValue, 0, readCount)
	if readCount > 0 {
		stCount := int(float64(readCount) * 0.6)
		atCount := readCount - stCount

		if atCount > 0 {
			readKVs = append(readKVs, r.selectRandomKVs(r.dataSet.AccountTries, atCount)...)
		}
		if stCount > 0 {
			readKVs = append(readKVs, r.selectRandomKVs(r.dataSet.StorageTries, stCount)...)
		}
		mathrand.Shuffle(len(readKVs), func(i, j int) { readKVs[i], readKVs[j] = readKVs[j], readKVs[i] })
	}
	task.ReadKVs = readKVs

	// Updates: sample uniformly from trie types according to updateCount
	if updateCount > 0 {
		// Split updates evenly between AccountTries and StorageTries
		accountUpdateCount := updateCount / 2
		storageUpdateCount := updateCount - accountUpdateCount // Handle odd updateCount
		up := make([]KeyValue, 0, updateCount)
		up = append(up, r.selectRandomKVs(r.dataSet.AccountTries, accountUpdateCount)...)
		up = append(up, r.selectRandomKVs(r.dataSet.StorageTries, storageUpdateCount)...)
		mathrand.Shuffle(len(up), func(i, j int) { up[i], up[j] = up[j], up[i] })
		task.UpdateKVs = up
	}

	// For WriteKVs, use trie types (AccountTries and StorageTries)
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
	if len(source) == 0 || count <= 0 {
		return nil
	}

	// 如果要选择的数量大于等于源数据长度，直接返回所有数据的随机排列
	if count >= len(source) {
		result := make([]KeyValue, len(source))
		copy(result, source)
		// Fisher-Yates 洗牌算法
		for i := len(result) - 1; i > 0; i-- {
			j := mathrand.Intn(i + 1)
			result[i], result[j] = result[j], result[i]
		}
		return result
	}
	// Floyd 算法：从 [0, n) 中无重复地等概率抽取 count 个索引，时间 O(count)，空间 O(count)
	n := len(source)
	picked := make(map[int]struct{}, count)
	for j := n - count; j < n; j++ { // j 递增，范围逐步放大
		t := mathrand.Intn(j + 1)   // [0, j]
		if _, ok := picked[t]; ok { // 若冲突，则选择 j
			picked[j] = struct{}{}
		} else {
			picked[t] = struct{}{}
		}
	}
	// 收集结果
	result := make([]KeyValue, 0, count)
	for idx := range picked {
		result = append(result, source[idx])
	}
	// 打乱输出顺序，避免索引集合遍历顺序带来的偏序
	mathrand.Shuffle(len(result), func(i, j int) { result[i], result[j] = result[j], result[i] })
	return result
}

func (r *PerfRunner) runInternal(ctx context.Context) {
	startTime := time.Now()
	warmupUntil := time.Time{}
	measuring := true
	if r.config.WarmupDur > 0 {
		warmupUntil = startTime.Add(r.config.WarmupDur)
		measuring = false
		log.Info("Warmup started", "duration", r.config.WarmupDur)
	}
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case task := <-r.taskChan:
			if task == nil {
				fmt.Println("Task channel closed, shutting down")
				r.printAVGStat(startTime)
				return
			}
			if measuring {
				r.processTask(task)
			} else {
				// Warmup: only perform reads, skip writes/updates
				if len(task.ReadKVs) > 0 {
					var readWG sync.WaitGroup
					r.processTrieReadsParallel(task.ReadKVs, &readWG)
					readWG.Wait()
				}
				// Note: don't accumulate writes or do updates in warmup
			}
			r.blockHeight++

			// Print average stats every 100 batches
			if r.blockHeight > 0 && r.blockHeight%100 == 0 {
				r.printAVGStat(startTime)
			}

		case <-ticker.C:
			if !measuring && !warmupUntil.IsZero() && time.Now().After(warmupUntil) {
				// Transition from warmup to measuring: reset counters
				r.resetCounters()
				measuring = true
				log.Info("Warmup finished; start measuring")
			}
			if measuring {
				r.printStat()
			} else {
				// During warmup, still print lightweight heartbeat
				fmt.Printf("[%s] Warmup in progress - block height=%d\n", time.Now().Format(time.RFC3339), r.blockHeight)
			}

		case <-ctx.Done():
			fmt.Println("Context cancelled, shutting down")
			r.printAVGStat(startTime)
			return
		}
	}
}

func (r *PerfRunner) processTask(task *Task) {

	// Process trie read operations
	if len(task.ReadKVs) > 0 {
		var readWG sync.WaitGroup
		r.processTrieReadsParallel(task.ReadKVs, &readWG)
		readWG.Wait()
	}

	// Process update operations sequentially - individual db.Put for each KV
	// Note: time is now measured inside processIndividualUpdates for each db.Put
	if len(task.UpdateKVs) > 0 {
		r.processIndividualUpdates(task.UpdateKVs)
	}

	// Accumulate write operations in memory for 256MB batch write
	if len(task.WriteKVs) > 0 {
		r.accumulateWrites(task.WriteKVs)
	}

}

func (r *PerfRunner) processTrieReadsParallel(readKVs []KeyValue, wg *sync.WaitGroup) {
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
			localOpTime := int64(0)

			for _, kv := range kvs {
				start := time.Now()
				_, err := r.db.Get(kv.Key)
				duration := time.Since(start)

				// Update min/max read times
				updateMinMaxDuration(&r.minTrieReadTime, &r.maxTrieReadTime, duration)
				// Update histogram (linear microsecond buckets)
				us := duration.Microseconds()
				if us < 0 {
					us = 0
				}
				b := us
				if b > latencyHistUSMax {
					b = latencyHistUSMax + 1
				}
				atomic.AddInt64(&r.trieLatencyHist[b], 1)

				if err != nil {
					// Key might not exist, continue
				}
				localReadOps++
				localOpTime += int64(duration)
			}

			atomic.AddInt64(&r.totalTrieReadOps, localReadOps)
			atomic.AddInt64(&r.totalReadOps, localReadOps) // Keep total counter for compatibility
			atomic.AddInt64((*int64)(&r.totalTrieReadTime), localOpTime)
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

				// Update min/max update times
				updateMinMaxDuration(&r.minUpdateTime, &r.maxUpdateTime, putDuration)
				// Update update latency histogram (linear microsecond buckets)
				us := putDuration.Microseconds()
				if us < 0 {
					us = 0
				}
				b := us
				if b > latencyHistUSMax {
					b = latencyHistUSMax + 1
				}
				atomic.AddInt64(&r.updateLatencyHist[b], 1)

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

	// Update min/max write times
	updateMinMaxDuration(&r.minWriteTime, &r.maxWriteTime, writeDuration)

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

// simpleShardIndex provides a simple hash-based sharding function
func simpleShardIndex(key []byte, shardNum int) int {
	if len(key) == 0 || shardNum <= 1 {
		return 0
	}
	// Use simple hash function for distribution
	var hash uint32
	for _, b := range key {
		hash = hash*31 + uint32(b)
	}
	return int(hash) % shardNum
}

// updateMinMaxDuration safely updates min and max duration values using atomic operations
func updateMinMaxDuration(currentMin, currentMax *int64, newDuration time.Duration) {
	newValue := int64(newDuration)

	// Update minimum (initialize to new value if currently 0, otherwise use minimum)
	for {
		currentMinValue := atomic.LoadInt64(currentMin)
		if currentMinValue == 0 || newValue < currentMinValue {
			if atomic.CompareAndSwapInt64(currentMin, currentMinValue, newValue) {
				break
			}
		} else {
			break
		}
	}

	// Update maximum
	for {
		currentMaxValue := atomic.LoadInt64(currentMax)
		if newValue > currentMaxValue {
			if atomic.CompareAndSwapInt64(currentMax, currentMaxValue, newValue) {
				break
			}
		} else {
			break
		}
	}
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

// formatDurationFromNanos formats duration from nanoseconds to appropriate unit
func formatDurationFromNanos(nanos int64) string {
	if nanos == 0 {
		return "0 μs"
	}
	duration := time.Duration(nanos)
	microseconds := float64(duration.Nanoseconds()) / 1000.0
	return formatLatency(microseconds)
}

func (r *PerfRunner) printStat() {
	// Calculate effective TPS (based on actual operation time)
	var trieReadTPS, updateTPS float64
	var trieReadLatency, writeLatency float64

	if r.totalTrieReadTime > 0 {
		trieReadTPS = float64(r.totalTrieReadOps) * float64(time.Second) / float64(r.totalTrieReadTime)
	}
	if r.totalUpdateTime > 0 {
		updateTPS = float64(r.totalUpdateOps) * float64(time.Second) / float64(r.totalUpdateTime)
	}

	// Calculate average latencies (in microseconds)
	if r.totalTrieReadOps > 0 {
		trieReadLatency = float64(r.totalTrieReadTime.Microseconds()) / float64(r.totalTrieReadOps)
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

	// No additional metrics updates needed - only latency timers are updated during operations

	// Get min/max values for display (atomic loads)
	trieMinTime := atomic.LoadInt64(&r.minTrieReadTime)
	trieMaxTime := atomic.LoadInt64(&r.maxTrieReadTime)
	updateMinTime := atomic.LoadInt64(&r.minUpdateTime)
	updateMaxTime := atomic.LoadInt64(&r.maxUpdateTime)
	writeMinTime := atomic.LoadInt64(&r.minWriteTime)
	writeMaxTime := atomic.LoadInt64(&r.maxWriteTime)

	fmt.Printf(
		"[%s] Perf In Progress - block height=%d\n"+
			"  Trie Read TPS: %.2f, Latency: %.2f μs (min: %s, max: %s)\n"+
			"  Update TPS: %.2f, Latency: %.2f μs (min: %s, max: %s)\n"+
			"  Write Batch: Latency: %s (min: %s, max: %s), Avg KVs: %.0f, Avg Size: %.1f MB, Count: %d\n"+
			"  Accumulated Updates: %d KVs, %.2f MB (target: 230-256MB)\n",
		time.Now().Format(time.RFC3339),
		r.blockHeight,
		trieReadTPS, trieReadLatency, formatDurationFromNanos(trieMinTime), formatDurationFromNanos(trieMaxTime),
		updateTPS, updateLatency, formatDurationFromNanos(updateMinTime), formatDurationFromNanos(updateMaxTime),
		formatLatency(writeLatency), formatDurationFromNanos(writeMinTime), formatDurationFromNanos(writeMaxTime),
		avgKVsPerBatch, avgBatchSizeMB, r.totalBatchWrites,
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

	var avgTrieReadLatency, avgWriteLatency, avgUpdateLatency float64

	if r.totalTrieReadOps > 0 {
		avgTrieReadLatency = float64(r.totalTrieReadTime.Microseconds()) / float64(r.totalTrieReadOps)
	}
	if r.totalBatchWrites > 0 {
		avgWriteLatency = float64(r.totalWriteTime.Microseconds()) / float64(r.totalBatchWrites)
	}
	if r.totalUpdateOps > 0 {
		avgUpdateLatency = float64(r.totalUpdateTime.Microseconds()) / float64(r.totalUpdateOps)
	}

	// Calculate effective TPS (based on actual operation time)
	var trieReadTPS, updateTPS float64
	if r.totalTrieReadTime > 0 {
		trieReadTPS = float64(r.totalTrieReadOps) * float64(time.Second) / float64(r.totalTrieReadTime)
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

	// Get final min/max values for display (atomic loads)
	trieMinTime := atomic.LoadInt64(&r.minTrieReadTime)
	trieMaxTime := atomic.LoadInt64(&r.maxTrieReadTime)
	updateMinTime := atomic.LoadInt64(&r.minUpdateTime)
	updateMaxTime := atomic.LoadInt64(&r.maxUpdateTime)
	writeMinTime := atomic.LoadInt64(&r.minWriteTime)
	writeMaxTime := atomic.LoadInt64(&r.maxWriteTime)

	tP50, tP95, tP99 := r.percentiles(r.trieLatencyHist[:], r.totalTrieReadOps)

	uP50, uP95, uP99 := r.percentiles(r.updateLatencyHist[:], r.totalUpdateOps)

	fmt.Printf(
		"=== Average Performance Metrics ===\n"+
			"Elapsed: %v, Block Height: %d\n"+
			"Trie Read   - Avg: %.2f μs, P50: %s, P95: %s, P99: %s, TPS: %.2f, Total: %d (min: %s, max: %s)\n"+
			"Update      - Avg: %.2f μs, P50: %s, P95: %s, P99: %s, TPS: %.2f, Total KVs: %d (min: %s, max: %s)\n"+
			"Write Batch - Avg: %s, Count: %d, Avg KVs: %.0f, Avg Size: %.1f MB (min: %s, max: %s)\n",
		elapsed,
		r.blockHeight,
		avgTrieReadLatency, tP50, tP95, tP99, trieReadTPS, r.totalTrieReadOps, formatDurationFromNanos(trieMinTime), formatDurationFromNanos(trieMaxTime),
		avgUpdateLatency, uP50, uP95, uP99, updateTPS, r.totalUpdateKVs, formatDurationFromNanos(updateMinTime), formatDurationFromNanos(updateMaxTime),
		formatLatency(avgWriteLatency), r.totalBatchWrites, avgKVsPerBatch, avgBatchSizeMB, formatDurationFromNanos(writeMinTime), formatDurationFromNanos(writeMaxTime),
	)

	if remainingKVs > 0 {
		fmt.Printf("Remaining Accumulated Updates: %d KVs, %.2f MB (will be flushed on shutdown)\n",
			remainingKVs, remainingAccumulatedMB)
	}
	fmt.Printf("===================================\n")
}

// percentiles computes P50/P95/P99 from a linear histogram where bucket i represents duration i microseconds.
// Buckets are linear: bucket[0] for 0us, bucket[1] for 1us, ..., bucket[5000] for 5000us, bucket[5001] for >5000us.
func (r *PerfRunner) percentiles(hist []int64, total int64) (string, string, string) {
	if total <= 0 {
		return "n/a", "n/a", "n/a"
	}
	// cumulative search
	p50Target := (total*50 + 99) / 100
	p95Target := (total*95 + 99) / 100
	p99Target := (total*99 + 99) / 100
	var cum int64
	p50Idx, p95Idx, p99Idx := 0, 0, 0
	for i := 0; i < len(hist); i++ {
		cum += atomic.LoadInt64(&hist[i])
		if p50Idx == 0 && cum >= p50Target {
			p50Idx = i
		}
		if p95Idx == 0 && cum >= p95Target {
			p95Idx = i
		}
		if p99Idx == 0 && cum >= p99Target {
			p99Idx = i
			break
		}
	}
	// Convert bucket index to representative duration (upper bound) in microseconds
	toDur := func(idx int) string {
		if idx <= 0 {
			return "0 μs"
		}
		if idx >= latencyHistUSMax+1 { // Overflow bucket
			return ">5000 μs"
		}
		return fmt.Sprintf("%d μs", idx)
	}
	return toDur(p50Idx), toDur(p95Idx), toDur(p99Idx)
}

// resetCounters clears all measuring counters, used after warmup finishes.
func (r *PerfRunner) resetCounters() {
	// Operation counts
	atomic.StoreInt64(&r.totalTrieReadOps, 0)
	atomic.StoreInt64(&r.totalUpdateOps, 0)
	atomic.StoreInt64(&r.totalWriteOps, 0)
	atomic.StoreInt64(&r.totalBatchWrites, 0)
	atomic.StoreInt64(&r.totalBatchSize, 0)

	// Timers
	r.totalTrieReadTime = 0
	r.totalUpdateTime = 0
	r.totalWriteTime = 0

	// Min/Max
	atomic.StoreInt64(&r.minTrieReadTime, 0)
	atomic.StoreInt64(&r.maxTrieReadTime, 0)
	atomic.StoreInt64(&r.minUpdateTime, 0)
	atomic.StoreInt64(&r.maxUpdateTime, 0)
	atomic.StoreInt64(&r.minWriteTime, 0)
	atomic.StoreInt64(&r.maxWriteTime, 0)

	// Interval baselines
	r.lastReadOps = 0
	r.lastWriteOps = 0
	r.lastUpdateOps = 0
	r.lastBatchCount = 0
	r.lastUpdateKVs = 0
	r.lastUpdateSize = 0
}

// makeConfigNode creates a simplified node configuration for database access
func makeConfigNode(ctx *cli.Context, benchDBPath string) (*node.Node, error) {
	// Create a completely clean configuration to avoid any default interference
	cfg := &node.Config{
		Name:      "geth",      // Set to "geth" for standard geth node behavior
		DataDir:   benchDBPath, // Use benchDBPath directly as data dir
		P2P:       node.DefaultConfig.P2P,
		HTTPPort:  node.DefaultConfig.HTTPPort,
		HTTPHost:  node.DefaultConfig.HTTPHost,
		WSPort:    node.DefaultConfig.WSPort,
		WSHost:    node.DefaultConfig.WSHost,
		LogConfig: node.DefaultConfig.LogConfig,
		// Enable sharding support if needed - copy default storage config
		Storage:        node.DefaultConfig.Storage,
		EnableSharding: true, // Enable sharding capability
	}

	log.Info("Creating node with clean config", "dataDir", cfg.DataDir, "name", cfg.Name, "enableSharding", cfg.EnableSharding)
	// Create the node
	stack, err := node.New(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create protocol stack: %v", err)
	}

	return stack, nil
}
