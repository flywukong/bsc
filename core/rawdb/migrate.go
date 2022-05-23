package rawdb

import (
	"context"
	"fmt"
	"github.com/ethereum/go-ethereum/ethdb/leveldb"
	"github.com/ethereum/go-ethereum/ethdb/remotedb"
	"github.com/go-redis/redis/v8"
	"os"
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

	DoneTaskNum uint64
)

var ctx = context.Background()

func InitDb() {
	var addr = []string{"127.0.0.1:6666", "127.0.0.1:6667"}
	path, _ := os.Getwd()
	persistCache, _ := leveldb.New(path+"/persistcache", 5000, 200, "chaindata", false)
	config := remotedb.DefaultConfig()
	config.Addrs = addr
	KvrocksDB, _ = remotedb.NewRocksDB(config, persistCache, false)
}

func (job *Job) UploadToKvRocks() error {
	fmt.Println("try to upload kv, batch size:", len(job.Kvbuffer))

	kvBatch := KvrocksDB.NewBatch()

	for key, value := range job.Kvbuffer {
		kvBatch.Put([]byte(key), value)
	}

	if err := kvBatch.Write(); err != nil {
		fmt.Println("send kv rocks error", err.Error())
		return err
	}
	/*
		for key, value := range job.Kvbuffer {
			err := rdb.Set(context.Background(), string(key), string(value), 0).Err()
			if err != nil {
				//	fmt.Println("send key:", string(key), "error")
				return err
			}
			//		fmt.Println("send key ", string(key), "finish")
		}
	*/

	fmt.Println("send batch finish")
	return nil
}

// A buffered channel that we can send work requests on.
var JobQueue chan Job

type Job struct {
	Kvbuffer map[string][]byte
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
				fmt.Println("receive job")
				if len(job.Kvbuffer) != 0 {
					fmt.Println("receive jobs, not empty1")
					if err := job.UploadToKvRocks(); err != nil {
						//	log.Error("Error uploading to kvrocks: %s", err.Error())
						fmt.Println("send kv rocks error")
					}
					incDoneTaskNum()
				}
				fmt.Println("receive job is empty")

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
	pool := make(chan chan Job, maxWorkers)
	return &Dispatcher{WorkerPool: pool, maxWorkers: maxWorkers,
		taskQueue: make(chan Job)}
}

func (d *Dispatcher) setTaskNum(num uint64) {
	fmt.Println("set task num", num)
	atomic.StoreUint64(&d.taskNum, num)
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

func (d *Dispatcher) SendKv(list map[string][]byte) {
	// let's create a job with the payload
	work := Job{list}
	// Push the work onto the queue.
	d.taskQueue <- work
}

func MigrateStart() *Dispatcher {
	dispatcher := NewDispatcher(1000)
	dispatcher.Run()
	fmt.Println("dispatcher begin run")
	return dispatcher
}

func (p *Dispatcher) Close() {
	// p.setStatus(STOPED) // 设置 status 为已停止
	time.Sleep(10 * time.Second)
	for {
		if GetDoneTaskNum() >= p.taskNum {
			fmt.Println("get tasknu enough", GetDoneTaskNum(), p.taskNum)
			break
		} else {
			fmt.Println("get tasknu not enough", GetDoneTaskNum(), p.taskNum)
			time.Sleep(10 * time.Second)
		}
	}

	close(p.taskQueue) // 关闭任务队列
}
