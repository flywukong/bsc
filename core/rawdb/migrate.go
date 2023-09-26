package rawdb

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	ethpebble "github.com/ethereum/go-ethereum/ethdb/pebble"
)

var (
	pebbleDB        *ethpebble.Database
	createErr       error
	DoneTaskNum     uint64
	SuccTaskNum     uint64
	FailTaskNum     uint64
	TaskFail        int64
	AncientTaskFail int64
)

var ctx = context.Background()

func InitDb(addr string) *ethpebble.Database {
	pebbleDB, createErr = ethpebble.New(addr, 4000, 4096, "eth/db/chaindata/", false)
	if createErr != nil {
		fmt.Println("create pebble err", createErr.Error())
		panic("create err")
	}
	return pebbleDB
}

func (job *Job) UploadToKvRocks() error {
	if len(job.Kvbuffer) > 0 {
		kvBatch := pebbleDB.NewBatch()
		for key, value := range job.Kvbuffer {
			batchErr := kvBatch.Put([]byte(key), value)
			if batchErr != nil {
				return batchErr
			}
		}

		if err := kvBatch.Write(); err != nil {
			fmt.Println("send kv rocks error", err.Error())
			return err
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
					fmt.Println("send kv rocks error", err.Error())
					if job.isAncient {
						MarkAncientTaskFail()
					} else {
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

func (d *Dispatcher) SendKv(list map[string][]byte, jobid uint64) {
	// let's create a job
	work := Job{list, jobid, false, nil, nil}
	// Push the work onto the queue.
	d.taskQueue <- work
}

func (d *Dispatcher) SendAncient(key []byte, val []byte, jobid uint64) {
	// let's create a job
	work := Job{nil, jobid, true, key, val}
	// Push the work onto the queue.
	d.taskQueue <- work
}

func MigrateStart(workersize uint64) *Dispatcher {
	dispatcher := NewDispatcher(workersize)
	dispatcher.Run()

	return dispatcher
}

func (p *Dispatcher) WaitDbFinish() bool {
	time.Sleep(3 * time.Second)
	for {
		if GetDoneTaskNum() >= p.taskNum {
			fmt.Println("get tasknu enough", GetDoneTaskNum(), p.taskNum)
			break
		} else {
			time.Sleep(3 * time.Second)
		}
	}
	if atomic.LoadInt64(&TaskFail) == 1 {
		return false
	}
	fmt.Println("level db migrate finish")
	return true
}

/*
func (d *Dispatcher) Close(checkErr bool) bool {
	defer close(d.taskQueue)
	defer close(d.StopCh)
	time.Sleep(3 * time.Second)
	for {
		if atomic.LoadInt64(&AncientTaskFail) == 1 {
			fmt.Println("ancient job fail, need restart ancient jobs")
			break
		}
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
		} else {
			atomic.StoreInt64(&TaskFail, 0)
			atomic.StoreInt64(&AncientTaskFail, 0)
		}
	}
	finish := false
	if doneAllTask && GetAncientFailFlag() == 0 && GetFailFlag() == 0 {
		finish = true
	}
	return finish
}
*/
