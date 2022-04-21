package state

import (
	"github.com/ethereum/go-ethereum/core/state/snapshot"
	"sync"

	"github.com/ethereum/go-ethereum/common"
)

// sharedPool is used to store maps of originStorage of stateObjects
type StoragePool struct {
	sync.RWMutex
	sharedMap map[common.Address]*sync.Map
}

type AccountPool struct {
	sync.RWMutex
	sharedMap map[common.Address]snapshot.Account
}

func NewStoragePool() *StoragePool {
	sharedMap := make(map[common.Address]*sync.Map)
	return &StoragePool{
		sync.RWMutex{},
		sharedMap,
	}
}

func NewAccountPool() *AccountPool {
	sharedMap := make(map[common.Address]snapshot.Account)
	return &AccountPool{
		sync.RWMutex{},
		sharedMap,
	}
}

// getStorage Check whether the storage exist in pool,
// new one if not exist, the content of storage will be fetched in stateObjects.GetCommittedState()
func (s *StoragePool) getStorage(address common.Address) *sync.Map {
	s.RLock()
	storageMap, ok := s.sharedMap[address]
	s.RUnlock()
	if !ok {
		s.Lock()
		defer s.Unlock()
		if storageMap, ok = s.sharedMap[address]; !ok {
			m := new(sync.Map)
			s.sharedMap[address] = m
			return m
		}
	}
	return storageMap
}

func (s *AccountPool) setAccount(address common.Address, account snapshot.Account) {
	s.Lock()
	defer s.Unlock()
	s.sharedMap[address] = account
}
func (s *AccountPool) getAccount(address common.Address) (snapshot.Account, bool) {
	s.RLock()
	defer s.RUnlock()
	account, ok := s.sharedMap[address]
	if !ok {
		return snapshot.Account{}, false
	}
	return account, true
}
