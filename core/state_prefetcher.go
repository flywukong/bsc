// Copyright 2019 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package core

import (
	"context"
	"github.com/ethereum/go-ethereum/cachemetrics"
	"github.com/ethereum/go-ethereum/consensus"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/ethereum/go-ethereum/params"
	"strconv"
	"sync/atomic"
	"time"
)

var (
	statePrefetchTimer       = metrics.NewRegisteredTimer("state/prefetch/delay", nil)
	statePrefetchCounter     = metrics.NewRegisteredCounter("state/prefetch/total", nil)
	statePrefetchFinishGauge = metrics.NewRegisteredGaugeFloat64("state/prefetch/txn/finish", nil)
	statePrefetchTotalGauge  = metrics.NewRegisteredGaugeFloat64("state/prefetch/txn/total", nil)
	statePrefetchFinishTimer = metrics.NewRegisteredGauge("state/prefetch/finish", nil)
	statePrefetchFail        = metrics.NewRegisteredGaugeFloat64("state/prefetch/fail/ratio", nil)
	//	statePrefetchFinishMeter  = metrics.NewRegisteredMeter("state/snapshot/dirty/storage/hit", nil)
)

const prefetchThread = 2

// statePrefetcher is a basic Prefetcher, which blindly executes a block on top
// of an arbitrary state with the goal of prefetching potentially useful state
// data from disk before the main block processor start executing.
type statePrefetcher struct {
	config *params.ChainConfig // Chain configuration options
	bc     *BlockChain         // Canonical block chain
	engine consensus.Engine    // Consensus engine used for block rewards
}

// NewStatePrefetcher initialises a new statePrefetcher.
func NewStatePrefetcher(config *params.ChainConfig, bc *BlockChain, engine consensus.Engine) *statePrefetcher {
	return &statePrefetcher{
		config: config,
		bc:     bc,
		engine: engine,
	}
}

// Prefetch processes the state changes according to the Ethereum rules by running
// the transaction messages using the statedb, but any changes are discarded. The
// only goal is to pre-cache transaction signatures and snapshot clean state.
func (p *statePrefetcher) Prefetch(block *types.Block, statedb *state.StateDB, cfg vm.Config, interrupt *uint32, done *bool) {
	var (
		header = block.Header()
		signer = types.MakeSigner(p.config, header.Number)
	)
	transactions := block.Transactions()
	sortTransactions := make([][]*types.Transaction, prefetchThread)
	for i := 0; i < prefetchThread; i++ {
		sortTransactions[i] = make([]*types.Transaction, 0, len(transactions)/prefetchThread)
	}
	for idx := range transactions {
		threadIdx := idx % prefetchThread
		sortTransactions[threadIdx] = append(sortTransactions[threadIdx], transactions[idx])
	}
	cachemetrics.UpdatePrefetchStartTime(time.Now().UnixNano())
	// No need to execute the first batch, since the main processor will do it.
	for i := 0; i < prefetchThread; i++ {
		go func(idx int, succ *bool) {
			newStatedb := statedb.Copy()
			newStatedb.EnableWriteOnSharedStorage()
			gaspool := new(GasPool).AddGas(block.GasLimit())
			blockContext := NewEVMBlockContext(header, p.bc, nil)
			evm := vm.NewEVM(blockContext, vm.TxContext{}, statedb, p.config, cfg)
			finish := 0
			// Calculate the current txn finish rate
			defer func() {
				toalTaskNum := float64(len(sortTransactions[idx]))
				statePrefetchFinishGauge.Update(float64(finish))
				statePrefetchTotalGauge.Update(toalTaskNum)
				statePrefetchFinishTimer.Update(time.Now().UnixNano())
			}()

			// Iterate over and process the individual transactions
			for i, tx := range sortTransactions[idx] {
				// If block precaching was interrupted, abort
				if interrupt != nil && atomic.LoadUint32(interrupt) == 1 {
					log.Info("interrupt, finish task:" + strconv.Itoa(finish) + "unfinish," +
						strconv.Itoa(len(sortTransactions[idx])-finish))
					statePrefetchFail.Update(float64(finish) / float64(len(sortTransactions[idx])))
					// cachemetrics.UpdatePrefetchTime(time.Now().UnixNano())
					*done = false
					return
				}
				// Convert the transaction into an executable message and pre-cache its sender
				msg, err := tx.AsMessage(signer)
				if err != nil {
					*done = false
					// cachemetrics.UpdatePrefetchTime(time.Now().UnixNano())
					return // Also invalid block, bail out
				}
				newStatedb.Prepare(tx.Hash(), header.Hash(), i)
				precacheTransaction(msg, p.config, gaspool, newStatedb, header, evm)
				finish++
			}
			*done = true
			cachemetrics.UpdatePrefetchTime(time.Now().UnixNano())
		}(i, done)
	}

}

// precacheTransaction attempts to apply a transaction to the given state database
// and uses the input parameters for its environment. The goal is not to execute
// the transaction successfully, rather to warm up touched data slots.
func precacheTransaction(msg types.Message, config *params.ChainConfig, gaspool *GasPool, statedb *state.StateDB, header *types.Header, evm *vm.EVM) {
	// Update the evm with the new transaction context.
	evm.Reset(NewEVMTxContext(msg), statedb)
	start := time.Now()
	// Add addresses to access list if applicable
	ApplyMessage(context.TODO(), evm, msg, gaspool)
	statePrefetchTimer.Update(time.Since(start))
	statePrefetchCounter.Inc(int64(time.Since(start)))
}
