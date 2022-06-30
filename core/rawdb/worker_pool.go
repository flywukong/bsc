package rawdb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb/leveldb"
	"github.com/ethereum/go-ethereum/ethdb/remotedb"
	"os"
	"strings"
	"sync/atomic"
	"time"
)

var (
	// MaxWorker = os.Getenv("MAX_WORKERS")
	//MaxQueue = os.Getenv("MAX_QUEUE")F
	//path, _         = os.Getwd()
	//persistCache, _ = leveldb.New(path+"/persistcache", 5000, 200, "chaindata", false)

	KvrocksDB *remotedb.RocksDB
	// kvrocksDB, _ = remotedb.NewRocksDB(remotedb.DefaultConfig2(addr), persistCache, false)

	searchHash       []byte
	DoneTaskNum      uint64
	SuccTaskNum      uint64
	FailTaskNum      uint64
	TaskFail         int64
	AncientTaskFail  int64
	errComPareKeyNum uint64
	rewriteCount     uint64
	ErrorDB          *leveldb.Database
)

var ctx = context.Background()

func InitDb(addr string) *remotedb.RocksDB {
	path, _ := os.Getwd()
	persistCache, _ := leveldb.New(path+"/persistcache", 5000, 200, "chaindata", false)
	config := remotedb.DefaultConfig()
	config.Addrs = strings.Split(addr, ",")
	KvrocksDB, _ = remotedb.NewRocksDB(config, persistCache, false)

	ErrorDB, _ = leveldb.New(path+"/error-comparedb", 100, 50,
		"chaindata", false)

	return KvrocksDB
}

type CompareError struct {
	errorCode string
	err       error
}

func (job *Job) CompareKvRocks() error {
	if job.isAncient {
		remoteValue, err := KvrocksDB.Get(job.ancientKey)

		if err != nil {
			fmt.Println("ancient could not find key,", string(job.ancientKey))
			return err
		}
		if bytes.Compare(remoteValue, job.ancientValue) != 0 {
			fmt.Println("ancient compare key error,", string(job.ancientKey))
			return errors.New("ancient compare not same")
		}
	} else {
		if len(job.Kvbuffer) > 0 {
			var keyList []string
			for key, _ := range job.Kvbuffer {
				keyList = append(keyList, key)
			}
			isSame := true

			// read values from kvrocks using pipeline
			valueList, err := KvrocksDB.PipeRead(keyList)
			if err != nil {
				fmt.Println("compare fail", err.Error())
				//return err
				isSame = false
			}

			if len(valueList) != len(keyList) {
				//	return errors.New("PipeRead key num error")
				isSame = false
			}

			// compare value one by one
			for i := 0; i < len(valueList); i++ {
				if bytes.Compare(job.Kvbuffer[keyList[i]], valueList[i]) != 0 {

					if keyList[i] == string(headHeaderKey) || keyList[i] == string(headBlockKey) ||
						keyList[i] == string(headFastBlockKey) || keyList[i] == string(lastPivotKey) {
						continue
					}

					if keyList[i] == string(databaseVersionKey) || keyList[i] == string(fastTrieProgressKey) ||
						keyList[i] == string(snapshotDisabledKey) || keyList[i] == string(snapshotRootKey) ||
						keyList[i] == string(snapshotJournalKey) || keyList[i] == string(snapshotGeneratorKey) ||
						keyList[i] == string(snapshotRecoveryKey) || keyList[i] == string(txIndexTailKey) ||
						keyList[i] == string(fastTxLookupLimitKey) || keyList[i] == string(uncleanShutdownKey) ||
						keyList[i] == string(badBlockKey) {
						continue
					}

					if bytes.HasPrefix([]byte(keyList[i]), []byte("parlia-")) && len(keyList[i]) == 7+common.HashLength {
						continue
					}

					if keyList[i] == "_globalCostFactorV6" {
						continue
					}

					isSame = false
					fmt.Println("compare key error, key:", keyList[i], "leveldb value:",
						string(job.Kvbuffer[keyList[i]]), "  vs:", string(valueList[i]))

					//fmt.Println("compare err, show bytes key:", keyList[i], "leveldb value:",
					//	job.Kvbuffer[keyList[i]], "  vs:", valueList[i])

					//	return errors.New("compare not same")
				}
			}
			// if compare batch not same rewrite the batch
			if isSame == false {
				fmt.Println("compare batch not same, need rewrite")
				incErrorNum()
				kvBatch := KvrocksDB.NewBatch()

				for key, value := range job.Kvbuffer {
					kvBatch.Put([]byte(key), value)
				}

				if batcherr := kvBatch.Write(); batcherr != nil {
					fmt.Println("rewrite kv rocks error", batcherr.Error(), "prefix:", job.prefix,
						"time:", time.Now().UTC().Format("2006-01-02 15:04:05"))
					for key, value := range job.Kvbuffer {
						err2 := ErrorDB.Put([]byte(key), value)
						if err2 != nil {
							fmt.Println("write to errordb fail")
						}
					}

				}
				fmt.Println("rewrite kv rocks finish", "prefix:", job.prefix,
					"time:", time.Now().UTC().Format("2006-01-02 15:04:05"))
			}
		}
	}
	return nil
}

// A buffered channel that we can send work requests on.
var JobQueue chan Job

type Job struct {
	Kvbuffer     map[string][]byte
	prefix       int
	isAncient    bool
	ancientKey   []byte
	ancientValue []byte
}

type Job2 struct {
	Kvbuffer     map[string][]byte
	JobId        uint64
	isAncient    bool
	ancientKey   []byte
	ancientValue []byte
}

// Worker represents the worker that executes the job
type Worker struct {
	WorkerPool chan chan Job
	JobChannel chan Job
	quit       chan bool
}

func NewWorker(workerPool chan chan Job) *Worker {
	return &Worker{
		WorkerPool: workerPool,
		JobChannel: make(chan Job),
		quit:       make(chan bool)}
}

// Start method starts the run loop for the worker, listening for a quit channel in
// case we need to stop it
func (w *Worker) Start() {
	go func() {
		for {
			// register the current worker into the worker queue.
			w.WorkerPool <- w.JobChannel

			select {
			case job := <-w.JobChannel:
				// compare batch
				if err := job.CompareKvRocks(); err != nil {
					if job.isAncient {
						fmt.Println("compare ancient error", err.Error())
						MarkAncientTaskFail()
					} else {
						//fmt.Println("compare kvrocks kv error", err.Error())
						MarkTaskFail()
					}
				}
				incDoneTaskNum()

			case <-w.quit:
				// we have received a signal to stop
				return
			}
		}
	}()
}

// Stop signals the worker to stop listening for work requests.
func (w Worker) Stop() {
	go func() {
		w.quit <- true
	}()
}

func initFailFlag() {
	atomic.StoreInt64(&TaskFail, 0)
	atomic.StoreInt64(&AncientTaskFail, 0)
}

func MarkTaskFail() {
	atomic.StoreInt64(&TaskFail, 1)
	atomic.AddUint64(&FailTaskNum, 1)
}

func MarkAncientTaskFail() {
	atomic.StoreInt64(&AncientTaskFail, 1)
}

func GetFailFlag() int64 {
	return atomic.LoadInt64(&TaskFail)
}

func GetAncientFailFlag() int64 {
	return atomic.LoadInt64(&AncientTaskFail)
}

type Dispatcher struct {
	// A pool of workers channels that are registered with the dispatcher
	WorkerPool chan chan Job
	maxWorkers uint64
	taskQueue  chan Job
	taskNum    uint64
	StopCh     chan struct{}
}

func incDoneTaskNum() { // runningWorkers + 1
	atomic.AddUint64(&DoneTaskNum, 1)
}

func incErrorNum() {
	atomic.AddUint64(&errComPareKeyNum, 1)
}

func GetDoneTaskNum() uint64 {
	return atomic.LoadUint64(&DoneTaskNum)
}

func ResetDoneTaskNum() {
	atomic.StoreUint64(&DoneTaskNum, 0)
}

func NewDispatcher(maxWorkers uint64) *Dispatcher {
	initFailFlag()
	pool := make(chan chan Job, maxWorkers)
	stopCh := make(chan struct{})
	return &Dispatcher{WorkerPool: pool, maxWorkers: maxWorkers,
		taskQueue: make(chan Job), StopCh: stopCh}
}

func (d *Dispatcher) setTaskNum(num uint64) {
	d.taskNum = num
}

func (d *Dispatcher) Run() {
	// starting n number of workers
	fmt.Println("dispatch run with worker num:", d.maxWorkers)

	for i := 0; i < int(d.maxWorkers); i++ {
		worker := NewWorker(d.WorkerPool)
		worker.Start()
	}

	go d.dispatch()
}

func (d *Dispatcher) dispatch() {
	for {
		select {
		case job := <-d.taskQueue:
			// a job request has been received
			go func(job Job) {
				// try to obtain a worker job channel that is available.
				// this will block until a worker is idle
				jobChannel := <-d.WorkerPool

				// dispatch the job to the worker job channel
				jobChannel <- job
			}(job)
		case <-d.StopCh:
			fmt.Println("dispatch stop")
			return
		}
	}
}

func (d *Dispatcher) SendKvJob(list map[string][]byte, prefix int, isAncient bool) {
	// let's create a job
	work := Job{list, prefix, isAncient, nil, nil}
	// Push the work onto the queue.
	d.taskQueue <- work
}

func (d *Dispatcher) SendAncientJob(key []byte, val []byte, jobid uint64, isAncient bool) {
	// let's create a job
	//	work := Job{nil, jobid, isAncient, key, val}
	// Push the work onto the queue.
	//  d.taskQueue <- work
	return
}

func CompareStart(workersize uint64) *Dispatcher {
	dispatcher := NewDispatcher(workersize)
	dispatcher.Run()
	atomic.StoreUint64(&rewriteCount, 0)
	atomic.StoreUint64(&errComPareKeyNum, 0)
	return dispatcher
}

func (p *Dispatcher) WaitDbFinish() uint64 {
	defer close(p.taskQueue)
	defer close(p.StopCh)
	time.Sleep(3 * time.Second)
	for {
		if GetDoneTaskNum() >= p.taskNum {
			fmt.Println("get tasknu enough", GetDoneTaskNum(), p.taskNum)
			break
		} else {
			time.Sleep(3 * time.Second)
		}
	}

	fmt.Println("get total compare error key num:", atomic.LoadUint64(&errComPareKeyNum))

	return atomic.LoadUint64(&errComPareKeyNum)
}

func (d *Dispatcher) Close(checkErr bool) bool {
	defer close(d.taskQueue)
	defer close(d.StopCh)
	// p.setStatus(STOPED) // 设置 status 为已停止
	time.Sleep(3 * time.Second)
	for {
		if atomic.LoadInt64(&AncientTaskFail) == 1 {
			fmt.Println("ancient job fail, need restry")
			break
		}
		if GetDoneTaskNum() >= d.taskNum {
			fmt.Println("finish jobs enough", GetDoneTaskNum(), d.taskNum)
			break
		} else {
			time.Sleep(3 * time.Second)
		}
	}
	// check if some task fail
	doneAllTask := true
	if checkErr {
		err := KvrocksDB.CheckError()
		if err != nil {
			// some data still not finish after retrying
			doneAllTask = false
		} else {
			atomic.StoreInt64(&TaskFail, 0)
			atomic.StoreInt64(&AncientTaskFail, 0)
		}
	}
	finish := false
	if doneAllTask && GetAncientFailFlag() == 0 && GetFailFlag() == 0 {
		finish = true
	}
	fmt.Println("get total compare error key num:", atomic.LoadUint64(&errComPareKeyNum))
	return finish
}
