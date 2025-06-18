package cachemetrics

import (
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/metrics"
)

type cacheLayerName string

const (
	//TotalRead cacheLayerName = "CACHE_L1_ACCOUNT"
	CacheL1ACCOUNT cacheLayerName = "CACHE_L1_ACCOUNT"
	CacheL2ACCOUNT cacheLayerName = "CACHE_L2_ACCOUNT"
	CacheL3ACCOUNT cacheLayerName = "CACHE_L3_ACCOUNT"
	DiskL4ACCOUNT  cacheLayerName = "DISK_L4_ACCOUNT"
	CacheL1STORAGE cacheLayerName = "CACHE_L1_STORAGE"
	CacheL2STORAGE cacheLayerName = "CACHE_L2_STORAGE"
	CacheL3STORAGE cacheLayerName = "CACHE_L3_STORAGE"
	DiskL4STORAGE  cacheLayerName = "DISK_L4_STORAGE"
)

var (
	cacheL1AccountTimer = metrics.NewRegisteredTimer("cache/cost/account/layer1", nil)
	cacheL2AccountTimer = metrics.NewRegisteredTimer("cache/cost/account/layer2", nil)
	cacheL3AccountTimer = metrics.NewRegisteredTimer("cache/cost/account/layer3", nil)
	diskL4AccountTimer  = metrics.NewRegisteredTimer("cache/cost/account/layer4", nil)
	cacheL1StorageTimer = metrics.NewRegisteredTimer("cache/cost/storage/layer1", nil)
	cacheL2StorageTimer = metrics.NewRegisteredTimer("cache/cost/storage/layer2", nil)
	cacheL3StorageTimer = metrics.NewRegisteredTimer("cache/cost/storage/layer3", nil)
	diskL4StorageTimer  = metrics.NewRegisteredTimer("cache/cost/storage/layer4", nil)

	cacheL1AccountCounter = metrics.NewRegisteredCounter("cache/count/account/layer1", nil)
	cacheL2AccountCounter = metrics.NewRegisteredCounter("cache/count/account/layer2", nil)
	cacheL3AccountCounter = metrics.NewRegisteredCounter("cache/count/account/layer3", nil)
	diskL4AccountCounter  = metrics.NewRegisteredCounter("cache/count/account/layer4", nil)
	cacheL1StorageCounter = metrics.NewRegisteredCounter("cache/count/storage/layer1", nil)
	cacheL2StorageCounter = metrics.NewRegisteredCounter("cache/count/storage/layer2", nil)
	cacheL3StorageCounter = metrics.NewRegisteredCounter("cache/count/storage/layer3", nil)
	diskL4StorageCounter  = metrics.NewRegisteredCounter("cache/count/storage/layer4", nil)

	cacheL1AccountCostCounter = metrics.NewRegisteredCounter("cache/totalcost/account/layer1", nil)
	cacheL2AccountCostCounter = metrics.NewRegisteredCounter("cache/totalcost/account/layer2", nil)
	cacheL3AccountCostCounter = metrics.NewRegisteredCounter("cache/totalcost/account/layer3", nil)
	diskL4AccountCostCounter  = metrics.NewRegisteredCounter("cache/totalcost/account/layer4", nil)
	cacheL1StorageCostCounter = metrics.NewRegisteredCounter("cache/totalcost/storage/layer1", nil)
	cacheL2StorageCostCounter = metrics.NewRegisteredCounter("cache/totalcost/storage/layer2", nil)
	cacheL3StorageCostCounter = metrics.NewRegisteredCounter("cache/totalcost/storage/layer3", nil)
	diskL4StorageCostCounter  = metrics.NewRegisteredCounter("cache/totalcost/storage/layer4", nil)

	// difflayer
	DiffLayerAccountReadCount int64
	DiffLayerAccountReadCost  time.Duration
	DiffLayerStorageReadCount int64
	DiffLayerStorageReadCost  time.Duration

	// disklayer
	DiskLayerAccountReadCount int64
	DiskLayerAccountReadCost  time.Duration
	DiskLayerStorageReadCount int64
	DiskLayerStorageReadCost  time.Duration

	// disklayer
	//DiskLayerAccountPebbleReadCount int64
	DiskLayerAccountPebbleReadCost time.Duration
	//DiskLayerStoragePebbleReadCount int64
	DiskLayerStoragePebbleReadCost time.Duration

	// block级别的diffLayer和diskLayer访问统计（Timer类型）
	BlockDiffLayerAccountReadCost   = metrics.NewRegisteredTimer("block/difflayer/account_read_cost", nil)
	BlockDiffLayerStorageReadCost   = metrics.NewRegisteredTimer("block/difflayer/storage_read_cost", nil)
	BlockDiskLayerAccountReadCost   = metrics.NewRegisteredTimer("block/disklayer/account_read_cost", nil)
	BlockDiskLayerStorageReadCost   = metrics.NewRegisteredTimer("block/disklayer/storage_read_cost", nil)
	BlockDiskLayerAccountPebbleCost = metrics.NewRegisteredTimer("block/disklayer/account_pebble_read_cost", nil)
	BlockDiskLayerStoragePebbleCost = metrics.NewRegisteredTimer("block/disklayer/storage_pebble_read_cost", nil)
)

// mark the info of total hit counts of each layers
func RecordCacheDepth(metricsName cacheLayerName) {
	switch metricsName {
	case CacheL1ACCOUNT:
		cacheL1AccountCounter.Inc(1)
	case CacheL2ACCOUNT:
		cacheL2AccountCounter.Inc(1)
	case CacheL3ACCOUNT:
		cacheL3AccountCounter.Inc(1)
	case DiskL4ACCOUNT:
		diskL4AccountCounter.Inc(1)
	case CacheL1STORAGE:
		cacheL1StorageCounter.Inc(1)
	case CacheL2STORAGE:
		cacheL2StorageCounter.Inc(1)
	case CacheL3STORAGE:
		cacheL3StorageCounter.Inc(1)
	case DiskL4STORAGE:
		diskL4StorageCounter.Inc(1)
	}
}

// mark the dalays of each layers
func RecordCacheMetrics(metricsName cacheLayerName, start time.Time) {
	switch metricsName {
	case CacheL1ACCOUNT:
		recordCost(cacheL1AccountTimer, start)
	case CacheL2ACCOUNT:
		recordCost(cacheL2AccountTimer, start)
	case CacheL3ACCOUNT:
		recordCost(cacheL3AccountTimer, start)
	case DiskL4ACCOUNT:
		recordCost(diskL4AccountTimer, start)
	case CacheL1STORAGE:
		recordCost(cacheL1StorageTimer, start)
	case CacheL2STORAGE:
		recordCost(cacheL2StorageTimer, start)
	case CacheL3STORAGE:
		recordCost(cacheL3StorageTimer, start)
	case DiskL4STORAGE:
		recordCost(diskL4StorageTimer, start)

	}
}

// accumulate the total dalays of each layers
func RecordTotalCosts(metricsName cacheLayerName, start time.Time) {
	switch metricsName {
	case CacheL1ACCOUNT:
		accumulateCost(cacheL1AccountCostCounter, start)
	case CacheL2ACCOUNT:
		accumulateCost(cacheL2AccountCostCounter, start)
	case CacheL3ACCOUNT:
		accumulateCost(cacheL3AccountCostCounter, start)
	case DiskL4ACCOUNT:
		accumulateCost(diskL4AccountCostCounter, start)
	case CacheL1STORAGE:
		accumulateCost(cacheL1StorageCostCounter, start)
	case CacheL2STORAGE:
		accumulateCost(cacheL2StorageCostCounter, start)
	case CacheL3STORAGE:
		accumulateCost(cacheL3StorageCostCounter, start)
	case DiskL4STORAGE:
		accumulateCost(diskL4StorageCostCounter, start)

	}
}

func recordCost(timer *metrics.Timer, start time.Time) {
	timer.Update(time.Since(start))
}

func accumulateCost(totalcost *metrics.Counter, start time.Time) {
	totalcost.Inc(time.Since(start).Nanoseconds())
}

// Reset all layer metrics (block开始前/结束后调用)
func ResetLayerMetrics() {
	atomic.StoreInt64(&DiffLayerAccountReadCount, 0)
	DiffLayerAccountReadCost = 0
	atomic.StoreInt64(&DiffLayerStorageReadCount, 0)
	DiffLayerStorageReadCost = 0
	atomic.StoreInt64(&DiskLayerAccountReadCount, 0)
	DiskLayerAccountReadCost = 0
	atomic.StoreInt64(&DiskLayerStorageReadCount, 0)
	DiskLayerStorageReadCost = 0
	// Pebble
	DiskLayerAccountPebbleReadCost = 0
	DiskLayerStoragePebbleReadCost = 0
}

// 累加方法
func AddDiffLayerAccountRead(cost time.Duration) {
	//atomic.AddInt64(&DiffLayerAccountReadCount, 1)
	DiffLayerAccountReadCost += cost
}
func AddDiffLayerStorageRead(cost time.Duration) {
	//atomic.AddInt64(&DiffLayerStorageReadCount, 1)
	DiffLayerStorageReadCost += cost
}
func AddDiskLayerAccountRead(cost time.Duration) {
	//atomic.AddInt64(&DiskLayerAccountReadCount, 1)
	DiskLayerAccountReadCost += cost
}
func AddDiskLayerStorageRead(cost time.Duration) {
	// atomic.AddInt64(&DiskLayerStorageReadCount, 1)
	DiskLayerStorageReadCost += cost
}
func AddDiskLayerAccountPebbleRead(cost time.Duration) {
	//atomic.AddInt64(&DiskLayerAccountReadCount, 1)
	DiskLayerAccountPebbleReadCost += cost
}
func AddDiskLayerStoragePebbleRead(cost time.Duration) {
	// atomic.AddInt64(&DiskLayerStorageReadCount, 1)
	DiskLayerStoragePebbleReadCost += cost
}
