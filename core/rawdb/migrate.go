package rawdb

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/ethereum/go-ethereum/ethdb/leveldb"
	"github.com/ethereum/go-ethereum/ethdb/remotedb"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	KvrocksDB       *remotedb.RocksDB
	DoneTaskNum     uint64
	SuccTaskNum     uint64
	FailTaskNum     uint64
	TaskFail        int64
	AncientTaskFail int64

	ErrorMap *RWMap
)

type RWMap struct {
	sync.RWMutex
	m map[int]uint64
}

// 新建一个RWMap
func NewRWMap(n int) *RWMap {
	return &RWMap{
		m: make(map[int]uint64, n),
	}
}

func (m *RWMap) addErrorNum(prefix int) {
	m.Lock() // 锁保护
	defer m.Unlock()
	m.m[prefix]++
}

func (m *RWMap) getErrorNum(prefix int) (uint64, bool) {
	m.RLock()
	defer m.RUnlock()
	v, existed := m.m[prefix] // 在锁的保护下从map中读取
	return v, existed
}

func (m *RWMap) MarkFail() (bool, uint64) { // 记录错误信息
	m.RLock() //遍历期间一直持有读锁
	defer m.RUnlock()
	finish := true
	errNum := uint64(0)
	for i := 0; i < 256; i++ {
		path, _ := os.Getwd()
		startDB, _ := leveldb.New(path+"/startdb"+strconv.Itoa(i), 5000, 200, "chaindata", false)
		if m.m[i] > 0 {
			finish = false
			errNum += m.m[i]
			startDB.Put([]byte("finish"), []byte("notdone"))
			// write err num
			var buf = make([]byte, 8)
			binary.BigEndian.PutUint64(buf, uint64(m.m[i]))
			startDB.Put([]byte("errorkey"), buf)
		} else {
			startDB.Put([]byte("finish"), []byte("done"))
		}

	}
	return finish, errNum
}

var ctx = context.Background()

func InitDb(addr string) *remotedb.RocksDB {
	path, _ := os.Getwd()
	persistCache, _ := leveldb.New(path+"/persistcache", 5000, 200, "chaindata", false)
	config := remotedb.DefaultConfig()
	config.Addrs = strings.Split(addr, ",")
	KvrocksDB, _ = remotedb.NewRocksDB(config, persistCache, false)

	ErrorMap = NewRWMap(256)
	return KvrocksDB
}

func (job *Job) UploadToKvRocks() error {
	if job.isAncient {
		err := KvrocksDB.Put(job.ancientKey, job.ancientValue)
		if err != nil {
			fmt.Println("send ancient kv error,", err.Error(), "time:", time.Now().UTC().Format("2006-01-02 15:04:05"))
			panic("ancient task fail")
			return err
		}
	} else {
		if len(job.Kvbuffer) > 0 {
			kvBatch := KvrocksDB.NewBatch()

			for key, value := range job.Kvbuffer {
				kvBatch.Put([]byte(key), value)
			}

			if err := kvBatch.Write(); err != nil {
				fmt.Println("send kv rocks error", err.Error(), "prefix:", job.prefix,
					"time:", time.Now().UTC().Format("2006-01-02 15:04:05"))
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
	prefix       int
	isAncient    bool
	ancientKey   []byte
	ancientValue []byte
}

type Job2 struct {
	Kvbuffer     map[string][]byte
	jobId        uint64
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
						ErrorMap.addErrorNum(job.prefix)
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

func (d *Dispatcher) SendKv(list map[string][]byte, prefix int) {
	// let's create a job
	work := Job{list, prefix, false, nil, nil}
	// Push the work onto the queue.
	d.taskQueue <- work
}

func (d *Dispatcher) SendAncient(key []byte, val []byte, jobid uint64) {
	// let's create a job
	// work := Job2{nil, jobid, true, key, val}
	// Push the work onto the queue.
	//d.taskQueue <- work
	return
}

func MigrateStart(workersize uint64) *Dispatcher {
	dispatcher := NewDispatcher(workersize)
	dispatcher.Run()

	return dispatcher
}

func (p *Dispatcher) WaitDbFinish() (bool, uint64) {
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

	allfinish, errNum := ErrorMap.MarkFail()

	//if atomic.LoadInt64(&TaskFail) == 1 {
	//		return false
	//	}
	// fmt.Println("level db migrate finish")
	return allfinish, errNum
}

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
