package monitorapi

import (
	"github.com/ethereum/go-ethereum/eth"
	"github.com/ethereum/go-ethereum/eth/tracers"
	"github.com/ethereum/go-ethereum/ethdb/remotedb"
	"github.com/ethereum/go-ethereum/metrics"
)

var APIBackendForHA *eth.EthAPIBackend
var RemoteDBConfig remotedb.Config
var MonitorAPI *tracers.API

var (
	alertGauge = metrics.NewRegisteredGauge("chain/archive/alert", nil)
)
