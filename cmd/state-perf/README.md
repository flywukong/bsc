# State Performance Testing Tool

A CLI-based performance testing tool for PebbleDB state operations in BSC (Binance Smart Chain).

## Features

- **Trie Data Processing**: Focuses on AccountTries and StorageTries data (snapshot support removed)
- **Sharding Database Support**: Supports both regular and sharding database modes  
- **Advanced Performance Metrics**: Includes TPS, latency measurements, and percentile statistics (P50, P95, P99)
- **Configurable Operations**: Adjustable read/write/update ratios for different test scenarios
- **Multi-threaded Operations**: Parallel processing with configurable thread count
- **Batch Write Operations**: Accumulates writes for 230-256MB batch operations
- **Trie Commit Measurement**: Periodic trie commit latency testing

## Building

```bash
cd cmd/state-perf
go build -o state-perf .
```

## Usage

### Basic Performance Test

```bash
./state-perf press-test --testcase /path/to/test-case --runtime 5m
```

### With Sharding Database

```bash
./state-perf press-test --testcase /path/to/test-case --sharding-db --runtime 10m
```

### Custom Configuration

```bash
./state-perf press-test \
  --testcase /path/to/test-case \
  --bench-db /path/to/bench-db \
  --batch 5000 \
  --read-ratio 0.6 \
  --write-ratio 0.2 \
  --update-ratio 0.2 \
  --threads 4 \
  --runtime 10m \
  --warmup 2m \
  --cache 4096 \
  --handles 32766 \
  --sharding-db
```

## Command Line Options

| Flag | Description | Default |
|------|-------------|---------|
| `--testcase, --tc` | Path to test-case pebble database directory | `test-case` |
| `--bench-db, --bdb` | Path for benchmark pebble database | `bench-pebble` |
| `--batch, --b` | Batch size for task generation | `5000` |
| `--seed` | Random seed (0 for random) | `0` |
| `--warmup` | Warmup duration before measuring | `10m` |
| `--read-ratio, --rr` | Read operations ratio (0.0-1.0) | `0.6` |
| `--write-ratio, --wr` | Write operations ratio (0.0-1.0) | `0.2` |
| `--update-ratio, --ur` | Update operations ratio (0.0-1.0) | `0.2` |
| `--threads, --t` | Number of worker threads | `1` |
| `--runtime, --rt` | Duration to run the benchmark | `60s` |
| `--cache` | Database cache size in MB | `4096` |
| `--handles` | Number of file descriptor handles | `32766` |
| `--sharding-db` | Enable sharding database mode | `false` |

## Operation Types

### Read Operations
- **Trie Reads**: 60% StorageTries, 40% AccountTries
- Parallel execution across multiple threads
- Real-time latency measurement and histogram collection

### Update Operations  
- Individual db.Put operations for each KV pair
- Evenly split between AccountTries and StorageTries
- Per-operation latency tracking

### Write Operations
- Accumulated batch writes (230-256MB batches)
- Key prefix preservation with random suffixes
- Value expansion (5x original size + random data)
- Asynchronous batch execution

## Performance Metrics

The tool provides comprehensive performance statistics:

- **TPS (Transactions Per Second)** - Based on actual operation time
- **Average Latency** - Mean operation duration  
- **Percentile Latency** - P50, P95, P99 percentiles
- **Min/Max Latency** - Operation duration bounds
- **Batch Statistics** - Write batch size, count, and throughput

## Output Example

```
[2024-09-03T17:15:30Z] Perf In Progress - block height=1250
  Trie Read TPS: 15420.50, Latency: 64.87 μs (min: 12 μs, max: 2.15 ms)
  Update TPS: 8234.12, Latency: 121.45 μs (min: 45 μs, max: 1.89 ms)  
  Write Batch: Latency: 156.78 ms (min: 98 ms, max: 284 ms), Avg KVs: 12450, Avg Size: 245.2 MB, Count: 15
  Accumulated Updates: 8450 KVs, 189.3 MB (target: 230-256MB)

=== Average Performance Metrics ===
Trie Read   - Avg: 65.23 μs, P50: 58 μs, P95: 124 μs, P99: 187 μs, TPS: 15320.40, Total: 1245000
Update      - Avg: 119.67 μs, P50: 95 μs, P95: 245 μs, P99: 412 μs, TPS: 8156.78, Total KVs: 456000  
Write Batch - Avg: 158.45 ms, Count: 15, Avg KVs: 12340, Avg Size: 244.8 MB
```

## Sharding Database Mode

When `--sharding-db` is enabled, the tool will:

1. Use the standard database interface initially
2. Configure sharding via `SetMultiDBs()` for state, snapshot, and index stores
3. Leverage BSC's sharding database implementation for improved performance
4. Distribute data across multiple shards based on key hashing

## Requirements

- BSC codebase with sharding database support
- Go 1.21+ (due to version compatibility in current environment)
- Test case directory with trie data (AccountTries and StorageTries)
- Sufficient disk space for benchmark database operations

## Notes

- The tool focuses exclusively on trie data processing (snapshot functionality removed)
- Sharding database is an experimental feature in BSC
- Large batch writes (230-256MB) may require substantial memory
- Performance results may vary based on hardware and database configuration
