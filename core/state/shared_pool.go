package state

import (
	"sync"

	"github.com/VictoriaMetrics/fastcache"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rlp"

	"github.com/ethereum/go-ethereum/common"
)

// StoragePool is used to store maps of originStorage of stateObjects
type StoragePool struct {
	sync.RWMutex
	sharedMap map[common.Address]*sync.Map
}

func NewStoragePool() *StoragePool {
	sharedMap := make(map[common.Address]*sync.Map)
	return &StoragePool{
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

type CacheAmongBlocks struct {
	// Cache among blocks
	cacheRoot     common.Hash
	aMux          sync.Mutex
	sMux          sync.Mutex
	accountsCache *fastcache.Cache
	storagesCache *fastcache.Cache
}

func NewCacheAmongBlocks() *CacheAmongBlocks {
	return &CacheAmongBlocks{
		cacheRoot:     types.EmptyRootHash,
		accountsCache: fastcache.New(20 * 1024 * 1024),
		storagesCache: fastcache.New(100 * 1024 * 1024),
	}
}

func NewCacheAmongBlocksWithCacheRoot(cacheRoot common.Hash) *CacheAmongBlocks {
	return &CacheAmongBlocks{
		cacheRoot:     cacheRoot,
		accountsCache: fastcache.New(10 * 1024 * 1024),
		storagesCache: fastcache.New(100 * 1024 * 1024),
	}
}

func (c *CacheAmongBlocks) GetRoot() common.Hash {
	return c.cacheRoot
}

func (c *CacheAmongBlocks) Purge() {
	c.storagesCache.Reset()
}

func (c *CacheAmongBlocks) Reset() {
	c.accountsCache.Reset()
	c.storagesCache.Reset()
	c.cacheRoot = types.EmptyRootHash
}

func (c *CacheAmongBlocks) SetRoot(root common.Hash) {
	c.cacheRoot = root
}

func (c *CacheAmongBlocks) GetAccount(key common.Hash) (*types.SlimAccount, bool) {
	if blob, found := c.accountsCache.HasGet(nil, key[:]); found {
		if len(blob) == 0 { // can be both nil and []byte{}
			return nil, true
		}
		account := new(types.SlimAccount)
		if err := rlp.DecodeBytes(blob, account); err != nil {
			panic(err)
		} else {
			return account, true
		}
	}
	return nil, false
}

func (c *CacheAmongBlocks) GetStorage(accountHash common.Hash, storageKey common.Hash) ([]byte, bool) {
	key := append(accountHash.Bytes(), storageKey.Bytes()...)
	if blob, found := c.storagesCache.HasGet(nil, key); found {
		return blob, true
	}
	return nil, false
}

/*
func (c *CacheAmongBlocks) SetAccount(key common.Hash, account *types.SlimAccount) {
	c.accountsCache.Set(key, account)
}

*/

func (c *CacheAmongBlocks) SetAccount(key common.Hash, account []byte) {
	c.accountsCache.Set(key[:], account)
}

func (c *CacheAmongBlocks) SetStorage(accountHash common.Hash, storageKey common.Hash, value []byte) {
	res := append(accountHash.Bytes(), storageKey.Bytes()...)
	c.storagesCache.Set(res, value)
}
