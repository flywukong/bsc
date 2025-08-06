# PebbleDB Benchmark Tool

一个基于以太坊 PebbleDB 的简单压测工具，用于测试数据库的读写性能。

## 功能特性

- 从 test-case 目录加载四种数据类型：accountTries、storageTries、accountSnaps、storageSnaps
- 支持自定义批处理大小
- 混合读写操作：60% 读取、20% 更新、20% 写入
- 实时性能监控和统计
- 每 100 个批次输出平均延迟和 TPS 统计

## 编译

```bash
go build -o pebble-bench ./cmd/pebble-bench
```

## 使用方法

### 基本用法

```bash
./pebble-bench -testcase ./test-case -batch 5000 -bench-db ./bench-pebble
```

### 参数说明

- `-testcase string`: test-case PebbleDB 数据库目录路径 (默认: "test-case")
- `-batch int`: 批处理大小 (默认: 5000)
- `-bench-db string`: 压测用 PebbleDB 数据库路径 (默认: "bench-pebble")

### 示例

```bash
# 使用默认参数
./pebble-bench

# 自定义批处理大小
./pebble-bench -batch 10000

# 指定数据源目录
./pebble-bench -testcase /path/to/test-case -batch 8000
```

## 工作流程

1. **数据加载**: 从 test-case 目录读取 PebbleDB 数据，按四种类型分类加载到内存
2. **任务生成**: 每个批次从四种数据类型各取 1/4 构造任务
3. **性能测试**: 对每个任务执行：
   - 60% 随机读操作
   - 20% 单个更新操作（使用 Put 接口）
   - 20% 批量写操作（使用 Batch 接口）
4. **统计输出**: 
   - 每 3 秒输出实时 TPS
   - 每 100 个批次输出平均延迟和总体 TPS

## 输出示例

### 实时统计（每 3 秒）
```
[2024-01-15T10:30:15Z] Perf In Progress - block height=25, Read TPS=15420.50, Write TPS=2580.25, Update TPS=2590.10
```

### 平均统计（每 100 批次）
```
=== Average Performance Metrics ===
Elapsed: 2m30s, Block Height: 100
Read  - Avg Latency: 45.20 μs, Total TPS: 12500.50
Write - Avg Latency: 120.80 μs, Total TPS: 2100.25
Update- Avg Latency: 95.30 μs, Total TPS: 2150.75
===================================
```

## 注意事项

1. 确保 test-case 目录存在且包含有效的 PebbleDB 数据
2. 工具会创建 bench-pebble 目录作为压测数据库
3. 使用 Ctrl+C 可以优雅关闭程序并输出最终统计
4. 建议在 SSD 硬盘上运行以获得更好的性能

## 数据类型说明

- **AccountTries**: 账户 Trie 节点数据
- **StorageTries**: 存储 Trie 节点数据  
- **AccountSnaps**: 账户快照数据
- **StorageSnaps**: 存储快照数据

每种数据类型在每个批次中各占 25%，确保测试的全面性和均衡性。 