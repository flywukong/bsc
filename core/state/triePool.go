package state

import (
	"fmt"
	"github.com/panjf2000/ants/v2"
	"time"
)

type TrieThreadPool ants.PoolWithFunc

var (
	BenchAntsSize = 1000
)

type TrieTask struct {
	fetcher *subfetcher
	start   time.Time
}

func (t *TrieTask) Do() {
	t.start = time.Now()
	t.fetcher.loop()
}

func taskFunc(data interface{}) {
	task := data.(*TrieTask)
	task.Do()
	//	fmt.Println("task start")
}

/*
func NewTriePool() (*TrieThreadPool, error) {
	pool, err := ants.NewPoolWithFunc(BenchAntsSize, taskFunc)
	if err != nil {
		fmt.Println("create pool fail")
		return nil, err
	}
	pool
	pool2 := (*TrieThreadPool)(pool)
	return pool2, nil
}
*/
func NewTriePool() (*ants.PoolWithFunc, error) {
	pool, err := ants.NewPoolWithFunc(BenchAntsSize, taskFunc)
	if err != nil {
		fmt.Println("create trie thread pool fail")
		return nil, err
	}
	fmt.Println("create trie thread pool done")
	return pool, nil
}
