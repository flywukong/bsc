package rawdb

import (
	"bytes"
	"context"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/ethdb/leveldb"
	"github.com/ethereum/go-ethereum/ethdb/remotedb"
	"github.com/go-redis/redis/v8"
	"os"
	"strings"
	"sync/atomic"
	"time"
)

var (
	// MaxWorker = os.Getenv("MAX_WORKERS")
	//MaxQueue = os.Getenv("MAX_QUEUE")
	rdb = redis.NewClient(&redis.Options{
		Addr:     "localhost:6666",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	//path, _         = os.Getwd()
	//persistCache, _ = leveldb.New(path+"/persistcache", 5000, 200, "chaindata", false)

	KvrocksDB *remotedb.RocksDB
	// kvrocksDB, _ = remotedb.NewRocksDB(remotedb.DefaultConfig2(addr), persistCache, false)

	searchHash      []byte
	DoneTaskNum     uint64
	SuccTaskNum     uint64
	FailTaskNum     uint64
	TaskFail        int64
	AncientTaskFail uint64
)

var ctx = context.Background()

func InitDb(addr string, db ethdb.Database) *remotedb.RocksDB {
	path, _ := os.Getwd()
	persistCache, _ := leveldb.New(path+"/persistcache", 5000, 200, "chaindata", false)
	config := remotedb.DefaultConfig()
	config.Addrs = strings.Split(addr, ",")
	KvrocksDB, _ = remotedb.NewRocksDB(config, persistCache, false)
	// get k,v from leveldb
	value, _ := db.Get(headHeaderKey)
	fmt.Println("db get headHeaderKey", string(value), "len", len(value))

	err1 := KvrocksDB.Put(headHeaderKey, value)
	if err1 != nil {
		fmt.Println("kvorocks set headHeaderKey fail")
	}
	testValue2, err2 := KvrocksDB.Get(headHeaderKey)
	if err2 != nil {
		fmt.Println("kvorocks get headHeaderKey fail")
	} else {
		fmt.Println("rocksdb get headHeaderKey", string(testValue2), "len", len(testValue2))
	}

	if bytes.Compare(value, testValue2) != 0 {
		fmt.Println("rocksdb set not same")
	}

	data11, _ := db.Get(headHeaderKey)

	hash_key_test := common.BytesToHash(data11)
	searchHash = headerNumberKey(hash_key_test)
	return KvrocksDB
}

func (job *Job) UploadToKvRocks() error {
	if job.isAncient {
		fmt.Println("is ancient")
		err := KvrocksDB.Put(job.ancientKey, job.ancientValue)
		if bytes.Compare(job.ancientKey, headHeaderKey) == 0 {
			fmt.Println("rocksdb set headHeaderKey", string(job.ancientValue), "len", len(job.ancientValue))
		}

		if bytes.Compare(job.ancientKey, searchHash) == 0 {
			fmt.Println("db get serchHash", string(job.ancientValue), "len", len(job.ancientValue))
		}

		var testKey string = "testkey"
		//var testValue string = "testvalue"
		if bytes.Compare(job.ancientKey, []byte(testKey)) == 0 {
			fmt.Println("rocksdb set testKey", string(job.ancientKey))
			fmt.Println("rocksdb set testValue", string(job.ancientValue))
		}

		if err != nil {
			fmt.Println("send kv error,", err.Error())
			return err
		}
	} else {
		fmt.Println("is ancient not")
		if len(job.Kvbuffer) > 0 {
			kvBatch := KvrocksDB.NewBatch()

			for key, value := range job.Kvbuffer {
				if bytes.Compare([]byte(key), headHeaderKey) == 0 {
					fmt.Println("rocksdb set headHeaderKey", string(value))
				}
				kvBatch.Put([]byte(key), value)
			}

			if err := kvBatch.Write(); err != nil {
				fmt.Println("send kv rocks error", err.Error())
				return err
			}
		}
	}
	return nil
}

// A buffered channel that we can send work requests on.
var JobQueue chan Job

type Job struct {
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
				// send batch to kvrocks
				if err := job.UploadToKvRocks(); err != nil {
					/*
						retrySucc := false
						// retry for 5 times
						for i := 0; i < 5; i++ {
							err = job.UploadToKvRocks()
							if err == nil {
								retrySucc = true
								fmt.Println("retry send kv rocks succ")
								break
							}
						}
						if !retrySucc {
							fmt.Println("send kv rocks error", err.Error())
							MarkTaskFail()
						}
					*/
					fmt.Println("send kv rocks error", err.Error())
					MarkTaskFail()
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
}

func MarkTaskFail() {
	atomic.StoreInt64(&TaskFail, 1)
	atomic.AddUint64(&FailTaskNum, 1)
}

func GetFailFlag() int64 {
	return atomic.LoadInt64(&TaskFail)
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

func (d *Dispatcher) SendKv(list map[string][]byte, jobid uint64, isAncient bool) {
	// let's create a job
	work := Job{list, jobid, isAncient, nil, nil}
	// Push the work onto the queue.
	d.taskQueue <- work
}

func (d *Dispatcher) SendKv2(key []byte, val []byte, jobid uint64, isAncient bool) {
	// let's create a job
	work := Job{nil, jobid, isAncient, key, val}
	// Push the work onto the queue.
	d.taskQueue <- work
}

func MigrateStart(workersize uint64) *Dispatcher {
	dispatcher := NewDispatcher(workersize)
	dispatcher.Run()

	return dispatcher
}

func (p *Dispatcher) WaitDbFinish() {
	time.Sleep(3 * time.Second)
	for {
		if GetDoneTaskNum() >= p.taskNum {
			fmt.Println("get tasknu enough", GetDoneTaskNum(), p.taskNum)
			break
		} else {
			time.Sleep(3 * time.Second)
		}
	}
}

func (d *Dispatcher) Close(checkErr bool) bool {
	defer close(d.taskQueue)
	defer close(d.StopCh)
	// p.setStatus(STOPED) // 设置 status 为已停止
	time.Sleep(3 * time.Second)
	for {
		if GetDoneTaskNum() >= d.taskNum {
			fmt.Println("get tasknu enough", GetDoneTaskNum(), d.taskNum)
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
		}
	}

	close(d.taskQueue) // close task queue
	if doneAllTask {
		fmt.Println("finish all migrate tasks")
	}
	return doneAllTask
}
