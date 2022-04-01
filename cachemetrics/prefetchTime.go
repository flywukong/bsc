package cachemetrics

import (
	"errors"
	"sync/atomic"
)

var (
	PrefetchStartTime  int64 // mining main process routine id
	BlockStartTime     int64 // syncing main process routine id
	PrefetchFinishTime int64 // mining main process routine id
	BlockFinishTime    int64 // syncing main process routine id
)

func UpdatePrefetchTime(time int64) {
	// log.Info("prefetch finish time: " + strconv.FormatInt(time, 10))
	atomic.StoreInt64(&PrefetchFinishTime, time)
}

func UpdateBlockTime(time int64) {
	// log.Info("block finish time: " + strconv.FormatInt(time, 10))
	atomic.StoreInt64(&BlockFinishTime, time)
}

func UpdatePrefetchStartTime(time int64) {
	// log.Info("prefetch finish time: " + strconv.FormatInt(time, 10))
	atomic.StoreInt64(&PrefetchStartTime, time)
}

func UpdateBlockStartTime(time int64) {
	// log.Info("block finish time: " + strconv.FormatInt(time, 10))
	atomic.StoreInt64(&BlockStartTime, time)
}

func GetPrefetchCost() int64 {
	time1 := atomic.LoadInt64(&PrefetchFinishTime)
	//	log.Info("prefetch finish time: " + strconv.FormatInt(time1, 10))
	time2 := atomic.LoadInt64(&PrefetchStartTime)

	return time1 - time2
}

func GetBlockCost() int64 {
	time1 := atomic.LoadInt64(&BlockFinishTime)
	//	log.Info("prefetch finish time: " + strconv.FormatInt(time1, 10))
	time2 := atomic.LoadInt64(&BlockStartTime)

	return time1 - time2
}

// 1. prefetch finish before block process, update the time
// 2. prefetch not finish, interrupt by main routine, return error
func GetDiffPrefetchBlock(finish bool) (int64, error) {
	if finish {
		time1 := atomic.LoadInt64(&PrefetchFinishTime)
		//	log.Info("prefetch finish time: " + strconv.FormatInt(time1, 10))
		time2 := atomic.LoadInt64(&BlockFinishTime)
		//	log.Info("diff block finish time: " + strconv.FormatInt(time2, 10))
		return (time2 - time1) / 1000, nil
	} else {
		return 0, errors.New("not perfetch finish")
	}
}
