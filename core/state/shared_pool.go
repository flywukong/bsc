package state

import (
	"sync"

	"github.com/ethereum/go-ethereum/common/lru"
	"github.com/ethereum/go-ethereum/core/types"

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
	accountsCache *lru.Cache[common.Hash, *types.SlimAccount]
	storagesCache *lru.Cache[string, []byte]
	//accountsCache *fastcache.Cache
	//storagesCache *fastcache.Cache
}

func NewCacheAmongBlocks() *CacheAmongBlocks {
	return &CacheAmongBlocks{
		cacheRoot:     types.EmptyRootHash,
		accountsCache: lru.NewCache[common.Hash, *types.SlimAccount](10000),
		storagesCache: lru.NewCache[string, []byte](250000),
		// accountsCache: fastcache.New(10000),
		// storagesCache: fastcache.New(10000),
	}
}

func (c *CacheAmongBlocks) GetRoot() common.Hash {
	return c.cacheRoot
}

func (c *CacheAmongBlocks) Purge() {
	//	c.accountsCache.Purge()
	c.storagesCache.Purge()
}

func (c *CacheAmongBlocks) SetRoot(root common.Hash) {
	c.cacheRoot = root
}

func (c *CacheAmongBlocks) GetAccount(key common.Hash) (*types.SlimAccount, bool) {
	//return c.accountsCache.HasGet(nil, key)
	return c.accountsCache.Get(key)
}

func (c *CacheAmongBlocks) GetAccountsNum() int {
	return len(c.accountsCache.Keys())
}

func (c *CacheAmongBlocks) GetStorageNum() int {
	return len(c.storagesCache.Keys())
}

func (c *CacheAmongBlocks) GetStorage(key string) ([]byte, bool) {
	//return c.storagesCache.HasGet(nil, key)
	return c.storagesCache.Get(key)
}

func (c *CacheAmongBlocks) SetAccount(key common.Hash, account *types.SlimAccount) {
	c.accountsCache.Add(key, account)
}

func (c *CacheAmongBlocks) SetStorage(key string, value []byte) {
	c.storagesCache.Add(key, value)
}
