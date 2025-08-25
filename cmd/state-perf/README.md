# State Performance Testing Tool

一个基于 CLI 框架的 PebbleDB 状态操作性能测试工具，支持灵活的读写比例配置和批处理大小调整。

## 功能特性

- 基于 CLI 框架，支持子命令和丰富的标志参数
- 从 test-case 目录加载四种状态数据类型
- 支持自定义读写比例（读取/写入/更新）
- 保持原始 key 前缀的智能 key 生成
- 实时性能监控和详细统计报告
- 支持运行时长控制和优雅退出

## 编译

```bash
cd cmd/state-perf
go build -o state-perf .
```

或使用 Makefile：
```bash
make build
```

## 使用方法

### 基本命令结构

```bash
./state-perf [global options] command [command options]
```

### 子命令

#### press-test
执行状态数据的压力测试：

```bash
./state-perf press-test [options]
```

### 全局选项

| 标志 | 短名 | 默认值 | 描述 |
|------|------|--------|------|
| `--testcase` | `-tc` | `test-case` | test-case PebbleDB 数据库目录路径 |
| `--bench-db` | `-bdb` | `bench-pebble` | 压测用 PebbleDB 数据库路径 |
| `--batch` | `-b` | `5000` | 批处理大小 |
| `--read-ratio` | `-rr` | `0.6` | 读操作比例 (0.0-1.0) |
| `--write-ratio` | `-wr` | `0.2` | 写操作比例 (0.0-1.0) |
| `--update-ratio` | `-ur` | `0.2` | 更新操作比例 (0.0-1.0) |
| `--threads` | `-t` | `1` | 工作线程数 |
| `--runtime` | `-rt` | `60s` | 测试运行时长 |
| `--metrics.addr` | `-ma` | `127.0.0.1` | 指标服务地址 |
| `--metrics.port` | `-mp` | `8545` | 指标服务端口 |

### 使用示例

#### 1. 基本压力测试
```bash
./state-perf press-test
```

#### 2. 自定义读写比例
```bash
# 70% 读取, 20% 写入, 10% 更新
./state-perf press-test --read-ratio 0.7 --write-ratio 0.2 --update-ratio 0.1
```

#### 3. 大批次长时间测试
```bash
./state-perf press-test --batch 10000 --runtime 5m
```

#### 4. 指定数据源和输出目录
```bash
./state-perf press-test --testcase ./my-test-case --bench-db ./my-bench-db
```

#### 5. 高强度写入测试
```bash
# 10% 读取, 80% 写入, 10% 更新
./state-perf press-test --read-ratio 0.1 --write-ratio 0.8 --update-ratio 0.1 --batch 8000
```

## 工作原理

> **重要说明**: 为了聚焦 Trie 数据结构的性能测试，写入操作仅使用 AccountTries 和 StorageTries 数据，而读取和更新操作使用所有四种数据类型以提供更真实的测试环境。

### 1. 数据加载
- 从指定的 test-case 目录读取 PebbleDB 数据
- 按四种类型分类：AccountTries、StorageTries、AccountSnaps、StorageSnaps
- 将数据加载到内存形成 data-set

### 2. 任务构造
- 每个批次从四种数据类型各取 1/4 构造混合任务
- 根据配置的比例分配读取、写入和更新操作
- **读取和更新操作**: 使用所有四种数据类型
- **写入操作**: 仅使用 Trie 类型数据（AccountTries 和 StorageTries）

### 3. Key 生成策略
写入操作时会生成新的 key，但保持与原始 key 相同的前缀：
- **AccountTries/StorageTries**: 保持前 8 字节作为前缀（**写入操作仅使用这两种类型**）
- **SnapshotAccount**: 保持 SnapshotAccountPrefix 作为前缀（仅用于读取和更新）
- **SnapshotStorage**: 保持 SnapshotStoragePrefix 作为前缀（仅用于读取和更新）
- **其他类型**: 保持前 4 字节作为前缀

### 4. 批处理写入优化
写入操作使用智能批处理机制：
- **单批大小控制**: 每个单独的 batch 严格控制在 230MB-256MB 范围内
- **自动分批**: 当单个 batch 达到 256MB 大小限制时自动提交并创建新 batch
- **最小阈值**: 确保每个 batch 至少达到 230MB 才提交（最后一个 batch 除外）
- **内存预留**: 每个 batch 额外预留 10% 空间给 PebbleDB 内部使用
- **多批处理**: 大任务自动分解为多个 230MB-256MB 的 batch 顺序执行
- **性能优化**: 大幅减少磁盘 I/O 次数，最大化写入吞吐量

### 5. 性能统计
- **实时统计**: 每 3 秒输出当前 TPS 和 batch 处理情况
  - `batches=156`: 总共处理的 batch 数量
  - `(interval: 12)`: 本次间隔内处理的 batch 数量
- **阶段统计**: 每 100 个批次输出平均延迟和累计 TPS
  - `Total Batches: 524`: 累计处理的 batch 总数
- **最终统计**: 程序结束时输出完整性能报告

## 输出示例

### 实时统计
```
[2024-01-15T10:30:15Z] Perf In Progress - block height=25, batches=156 (interval: 12), Read TPS=15420.50, Write TPS=516.05, Update TPS=258.03
```

### 阶段统计
```
=== Average Performance Metrics ===
Elapsed: 2m30s, Block Height: 100, Total Batches: 524
Read  - Avg Latency: 45.20 μs, Total TPS: 12500.50, Total Ops: 1875075
Write - Avg Latency: 180.60 μs, Total TPS: 416.75, Total Ops: 62512
Update- Avg Latency: 165.30 μs, Total TPS: 208.38, Total Ops: 31256
===================================
```

## 比例验证

工具会自动验证读写比例的总和是否等于 1.0（允许 ±0.01 的误差）：

```bash
# 错误示例 - 比例总和不等于 1.0
./state-perf press-test --read-ratio 0.6 --write-ratio 0.3 --update-ratio 0.2
# Error: read-ratio + write-ratio + update-ratio must equal 1.0, got 1.10
```

## 注意事项

1. **数据源**: 确保 test-case 目录存在且包含有效的 PebbleDB 数据
2. **磁盘空间**: 写入操作会创建新数据，确保有足够的磁盘空间
3. **运行时长**: 可以使用 Ctrl+C 提前终止测试
4. **线程数**: 当前版本为单线程运行，多线程支持在开发中
5. **前缀保持**: 新生成的 key 会保持原始前缀，确保数据类型一致性

## 性能调优建议

- **SSD 存储**: 在 SSD 上运行可获得更好的性能
- **批次大小**: 较大的批次可能提高吞吐量但增加延迟
- **读写比例**: 根据实际应用场景调整比例
- **运行时长**: 建议至少运行 5 分钟以获得稳定的性能数据 