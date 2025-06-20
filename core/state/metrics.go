// Copyright 2021 The go-ethereum Authors
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

package state

import "github.com/ethereum/go-ethereum/metrics"

var (
	accountReadMeters          = metrics.NewRegisteredMeter("state/read/accounts", nil)
	storageReadMeters          = metrics.NewRegisteredMeter("state/read/storage", nil)
	readerStorageAccessMeter   = metrics.NewRegisteredMeter("state/reader/storage/access", nil)
	readerStorageAccessGauge   = metrics.NewRegisteredGauge("state/reader/storage/access/gauge", nil)
	readerIOStorageAccessGauge = metrics.NewRegisteredGauge("state/reader/storage/io/gauge", nil)

	accountUpdatedMeter      = metrics.NewRegisteredMeter("state/update/account", nil)
	storageUpdatedMeter      = metrics.NewRegisteredMeter("state/update/storage", nil)
	accountDeletedMeter      = metrics.NewRegisteredMeter("state/delete/account", nil)
	storageDeletedMeter      = metrics.NewRegisteredMeter("state/delete/storage", nil)
	accountTrieUpdatedMeter  = metrics.NewRegisteredMeter("state/update/accountnodes", nil)
	storageTriesUpdatedMeter = metrics.NewRegisteredMeter("state/update/storagenodes", nil)
	accountTrieDeletedMeter  = metrics.NewRegisteredMeter("state/delete/accountnodes", nil)
	storageTriesDeletedMeter = metrics.NewRegisteredMeter("state/delete/storagenodes", nil)
	TotalGetBalanceCost      = metrics.NewRegisteredTimer("state/getbalance/total", nil)
	TotalGetNonceCost        = metrics.NewRegisteredTimer("state/getnonce/total", nil)
	TotalGetStorageRootCost  = metrics.NewRegisteredTimer("state/getstorageroot/total", nil)
	TotalGetCodeCost         = metrics.NewRegisteredTimer("state/getcode/total", nil)
	TotalGetCodeSizeCost     = metrics.NewRegisteredTimer("state/getcodesize/total", nil)
	TotalGetCodeHashCost     = metrics.NewRegisteredTimer("state/getcodehash/total", nil)
	TotalGetStateCost        = metrics.NewRegisteredTimer("state/getstate/total", nil)
)
