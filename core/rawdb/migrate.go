package rawdb

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
)

var (
	// MaxWorker = os.Getenv("MAX_WORKERS")
	//MaxQueue = os.Getenv("MAX_QUEUE")
	rdb = redis.NewClient(&redis.Options{
		Addr:     "localhost:6666",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
)
var ctx = context.Background()

func (job *Job) UploadToKvRocks() error {
	for key, value := range job.Kvbuffer {
		err := rdb.Set(context.Background(), string(key), string(value), 0).Err()
		if err != nil {
			fmt.Println("send key:", string(key), "error")
			return err
		}
		fmt.Println("send key ", string(key), "finish")
	}
	return nil
}

// A buffered channel that we can send work requests on.
var JobQueue chan Job

type Job struct {
	Kvbuffer map[string]string
}

// Worker represents the worker that executes the job
type Worker struct {
	WorkerPool chan chan Job
	JobChannel chan Job
	quit       chan bool
}

func NewWorker(workerPool chan chan Job) Worker {
	return Worker{
		WorkerPool: workerPool,
		JobChannel: make(chan Job),
		quit:       make(chan bool)}
}

// Start method starts the run loop for the worker, listening for a quit channel in
// case we need to stop it
func (w Worker) Start() {
	go func() {
		for {
			// register the current worker into the worker queue.
			w.WorkerPool <- w.JobChannel

			select {
			case job := <-w.JobChannel:
				// we have received a work request.
				if err := job.UploadToKvRocks(); err != nil {
					//	log.Error("Error uploading to kvrocks: %s", err.Error())
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

type Dispatcher struct {
	// A pool of workers channels that are registered with the dispatcher
	WorkerPool chan chan Job
	maxWorkers int
}

func NewDispatcher(maxWorkers int) *Dispatcher {
	pool := make(chan chan Job, maxWorkers)
	return &Dispatcher{WorkerPool: pool}
}

func (d *Dispatcher) Run() {
	// starting n number of workers
	for i := 0; i < d.maxWorkers; i++ {
		worker := NewWorker(d.WorkerPool)
		worker.Start()
	}

	go d.dispatch()
}

func (d *Dispatcher) dispatch() {
	for {
		select {
		case job := <-JobQueue:
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

func SendKv(list map[string]string) {
	// let's create a job with the payload
	work := Job{list}

	// Push the work onto the queue.
	JobQueue <- work
}

func MigrateStart() {
	dispatcher := NewDispatcher(1000)
	dispatcher.Run()
}

/*
var (
	BenchAntsSize = 1000
)

type Task struct {
	kvlist map[string][]byte
	client redis.Client
}

func (t *Task) Do() {
	// t.start = time.Now()
	//t.fetcher.loop()
}

func taskFunc(data interface{}) {
	task := data.(*Task)
	task.Do()
	//	fmt.Println("task start")
}

func NewTaskPool() (*ants.PoolWithFunc, error) {
	pool, err := ants.NewPoolWithFunc(BenchAntsSize, taskFunc)
	if err != nil {
		fmt.Println("create thread pool fail")
		return nil, err
	}
	fmt.Println("create thread pool done")
	return pool, nil
}
*/
