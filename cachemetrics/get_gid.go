package cachemetrics

import (
	"fmt"
	"github.com/petermattis/goid"
	"strconv"
	"time"
)

var (
	MiningRoutineId  int64
	SyncingRoutineId int64
)

func Goid() int64 {
	start := time.Now()
	defer func() {
		time := time.Since(start)
		fmt.Println("get cost time:" + strconv.FormatInt(time.Nanoseconds(), 10))
	}()
	return goid.Get()
}

func UpdateMiningRoutineID(id int64) {
	MiningRoutineId = id
}

func UpdateSyncingRoutineID(id int64) {
	SyncingRoutineId = id
}

func getMiningRoutineID() int64 {
	return MiningRoutineId
}

func getSyncingRoutineID() int64 {
	return SyncingRoutineId
}

func isMainRoutineID(id int64) bool {
	if id == MiningRoutineId || id == SyncingRoutineId {
		return true
	}
	return false
}