package cachemetrics

import (
	"fmt"
	"github.com/ethereum/go-ethereum/log"
	"github.com/petermattis/goid"
	"runtime"
	"strconv"
	"strings"
	"time"
)

var (
	MiningRoutineId  uint64
	SyncingRoutineId uint64
)

func Goid() (uint64, error) {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	idField := strings.Fields(strings.TrimPrefix(string(buf[:n]), "goroutine "))[0]
	//id, err := strconv.Atoi(idField)
	id, err := strconv.ParseUint(string(idField), 10, 64)
	if err != nil {
		log.Error("get goroutine id error", err.Error())
		return 0, err
	}
	//log.Info("get main loop goroutine id :"+ id)
	return id, nil
}

func Goid2() int64 {
	start := time.Now()
	defer func() {
		time := time.Since(start)
		fmt.Println("cost time3:" + strconv.FormatInt(time.Nanoseconds(), 10))
	}()
	return goid.Get()
}

func UpdateMiningRoutineID(id uint64) {
	MiningRoutineId = id
}

func UpdateSyncingRoutineID(id uint64) {
	SyncingRoutineId = id
}

func getMiningRoutineID() uint64 {
	return MiningRoutineId
}

func getSyncingRoutineID() uint64 {
	return SyncingRoutineId
}
