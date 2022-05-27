package rawdb

import (
	"context"
	"fmt"
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

	DoneTaskNum     uint64
	SuccTaskNum     uint64
	FailTaskNum     uint64
	TaskFail        int64
	AncientTaskFail uint64
)

var ctx = context.Background()

func InitDb(addr string) {
	path, _ := os.Getwd()
	persistCache, _ := leveldb.New(path+"/persistcache", 5000, 200, "chaindata", false)
	config := remotedb.DefaultConfig()
	config.Addrs = strings.Split(addr, ",")
	KvrocksDB, _ = remotedb.NewRocksDB(config, persistCache, false)
}

func (job *Job) UploadToKvRocks() error {
	kvBatch := KvrocksDB.NewBatch()

	for key, value := range job.Kvbuffer {
		kvBatch.Put([]byte(key), value)
	}

	if err := kvBatch.Write(); err != nil {
		fmt.Println("send kv rocks error", err.Error())
		return err
	}
	return nil
}

// A buffered channel that we can send work requests on.
var JobQueue chan Job

type Job struct {
	Kvbuffer  map[string][]byte
	JobId     uint64
	isAncient bool
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
				if len(job.Kvbuffer) != 0 {
					if err := job.UploadToKvRocks(); err != nil {
						retrySucc := false
						// retry for 5 times
						for i := 0; i < 5; i++ {
							err = job.UploadToKvRocks()
							if err == nil {
								retrySucc = true
								break
							}
						}
						if !retrySucc {
							fmt.Println("send kv rocks error", err.Error())
							MarkTaskFail()
						}
					}
					incDoneTaskNum()
				}

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
}

func incDoneTaskNum() { // runningWorkers + 1
	atomic.AddUint64(&DoneTaskNum, 1)
}

func GetDoneTaskNum() uint64 {
	return atomic.LoadUint64(&DoneTaskNum)
}

func NewDispatcher(maxWorkers uint64) *Dispatcher {
	initFailFlag()
	pool := make(chan chan Job, maxWorkers)
	return &Dispatcher{WorkerPool: pool, maxWorkers: maxWorkers,
		taskQueue: make(chan Job)}
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
		}
	}
}

func (d *Dispatcher) SendKv(list map[string][]byte, jobid uint64, isAncient bool) {
	// let's create a job with the payload
	work := Job{list, jobid, isAncient}
	// Push the work onto the queue.
	d.taskQueue <- work
}

func MigrateStart(workersize uint64) *Dispatcher {
	dispatcher := NewDispatcher(workersize)
	dispatcher.Run()

	return dispatcher
}

func (p *Dispatcher) Close(checkErr bool) bool {
	// p.setStatus(STOPED) // 设置 status 为已停止
	time.Sleep(3 * time.Second)
	for {
		if GetDoneTaskNum() >= p.taskNum {
			fmt.Println("get tasknu enough", GetDoneTaskNum(), p.taskNum)
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

	close(p.taskQueue) // close task queue
	if doneAllTask {
		fmt.Println("finish all migrate tasks")
	}
	return doneAllTask
}
