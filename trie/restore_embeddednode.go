package trie

import (
	"bytes"
	"errors"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state/snapshot"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/triedb"
	"golang.org/x/crypto/sha3"
)

const ExpectLeafNodeLen = 32

type EmbeddedNodeRestorer struct {
	db   ethdb.Database
	stat *dbNodeStat
}

type dbNodeStat struct {
	ShortNodeCnt    uint64
	ValueNodeCnt    uint64
	FullNodeCnt     uint64
	EmbeddedNodeCnt uint64
}

func NewEmbeddedNodeRestorer(chaindb ethdb.Database) *EmbeddedNodeRestorer {
	return &EmbeddedNodeRestorer{
		db:   chaindb,
		stat: &dbNodeStat{0, 0, 0, 0},
	}
}

// hasher has some confict with hasher inside trie package, temporarily copy a hasher from rawdb
type dbhasher struct{ sha crypto.KeccakState }

var dbhasherPool = sync.Pool{
	New: func() interface{} { return &dbhasher{sha: sha3.NewLegacyKeccak256().(crypto.KeccakState)} },
}

func newdbHasher() *dbhasher {
	return dbhasherPool.Get().(*dbhasher)
}

func (h *dbhasher) hash(data []byte) common.Hash {
	return crypto.HashData(h.sha, data)
}

func (h *dbhasher) release() {
	hasherPool.Put(h)
}

type shorNodeInfo struct {
	NodeBytes []byte
	Idx       int
}

func checkIfContainShortNode(hash, key, buf []byte, stat *dbNodeStat) ([]shorNodeInfo, error) {
	n, err := decodeNode(hash, buf)
	if err != nil {
		return nil, err
	}

	shortNodeInfoList := make([]shorNodeInfo, 0)
	if fn, ok := n.(*fullNode); ok {
		stat.FullNodeCnt++
		// find shortNode inside full node
		for i := 0; i < 17; i++ {
			child := fn.Children[i]
			if sn, ok := child.(*shortNode); ok {
				if i == 16 {
					panic("should not exist child[17] in secure trie")
				}
				if vn, ok := sn.Val.(valueNode); ok {
					if rawdb.IsStorageTrieNode(key) {
						// full node path
						valueNodePath := key[1+common.HashLength:]
						// add node index to path
						valueNodePath = append(valueNodePath, byte(i))
						// check the length of node path
						if len(hexToKeybytes(append(valueNodePath, sn.Key...))) == ExpectLeafNodeLen {
							log.Info("found short leaf Node inside full node", "full node info", fn, "child idx", i,
								"child", child, "value", vn)
							stat.EmbeddedNodeCnt++
							shortNodeInfoList = append(shortNodeInfoList,
								shorNodeInfo{NodeBytes: nodeToBytes(child), Idx: i})
						}
					}
				}
			}
		}
		return shortNodeInfoList, nil
	} else if sn, ok := n.(*shortNode); ok {
		stat.ShortNodeCnt++
		if _, ok := sn.Val.(valueNode); ok {
			if rawdb.IsStorageTrieNode(key) {
				shortNodePath := key[1+common.HashLength:]
				shortNodePath = append(shortNodePath, sn.Key...)
				if len(hexToKeybytes(shortNodePath)) == ExpectLeafNodeLen {
					stat.ValueNodeCnt++
				}
			} else {
				shortNodePath := key[1:]
				shortNodePath = append(shortNodePath, sn.Key...)
				if len(hexToKeybytes(shortNodePath)) == ExpectLeafNodeLen {
					stat.ValueNodeCnt++
				}
			}
		}
	} else {
		log.Warn("not full node or short node in disk", "node", n)
	}
	return nil, nil
}

func (restorer *EmbeddedNodeRestorer) Run() error {
	var (
		it     ethdb.Iterator
		start  = time.Now()
		logged = time.Now()
		batch  = restorer.db.NewBatch()
		count  int64
		key    []byte
	)

	prefixKeys := map[string]func([]byte) bool{
		string(rawdb.TrieNodeAccountPrefix): rawdb.IsAccountTrieNode,
		string(rawdb.TrieNodeStoragePrefix): rawdb.IsStorageTrieNode,
	}

	var storageEmbeddedNode int
	var accountEmbeddedNode int

	// todo no need AccountPrefix iterator
	for prefix, isValid := range prefixKeys {
		it = restorer.db.NewIterator([]byte(prefix), nil)
		for it.Next() {
			key = it.Key()
			if !isValid(key) {
				continue
			}

			h := rawdb.NewSha256Hasher()
			hash := h.Hash(it.Value())
			h.Release()
			var childPath []byte
			// if is full short node InsideFull, check if it contains short shortnodeInsideFull
			shortnodeList, err := checkIfContainShortNode(hash.Bytes(), key, it.Value(), restorer.stat)
			if err != nil {
				log.Error("decode trie shortnode inside fullnode err:", "err", err.Error())
				return err
			}

			// find shorNode inside the fullnode
			if len(shortnodeList) > 0 {
				if len(shortnodeList) > 1 {
					log.Info("fullnode contain more than 1 short node", "short node num", len(shortnodeList))
				}
				for _, snode := range shortnodeList {
					if rawdb.IsStorageTrieNode(key) {
						storageEmbeddedNode++
						fullNodePath := key[1+common.HashLength:]
						childPath = append(fullNodePath, byte(snode.Idx))
						newKey := append(key, byte(snode.Idx))
						log.Info("storage shortNode info", "trie key", key, "fullNode path", fullNodePath,
							"child path", childPath, "new node key", newKey, "new node value", snode.NodeBytes)
						// batch write
						if err := batch.Put(newKey, snode.NodeBytes); err != nil {
							return err
						}

					} else if rawdb.IsAccountTrieNode(key) {
						// should not contain account embedded node
						accountEmbeddedNode++
						fullNodePath := key[1:]
						childPath = append(fullNodePath, byte(snode.Idx))
						newKey := append(key, byte(snode.Idx))
						log.Warn("account shortNode info", "trie key", key, "fullNode path", fullNodePath,
							"child path", childPath, "new Storage key", newKey, "new node value", snode.NodeBytes)

					}
				}
			}

			if batch.ValueSize() > ethdb.IdealBatchSize {
				if err := batch.Write(); err != nil {
					it.Release()
					return err
				}
				batch.Reset()
			}

			count++
			if time.Since(logged) > 8*time.Second {
				log.Info("Checking trie state", "count", count, "elapsed", common.PrettyDuration(time.Since(start)))
				logged = time.Now()
			}
		}
		it.Release()
	}

	log.Info("embedded node info", "storage embedded node", storageEmbeddedNode, "account embedded node", accountEmbeddedNode)
	log.Info(" total node info", "fullnode count", restorer.stat.FullNodeCnt,
		"short node count", restorer.stat.ShortNodeCnt, "value node", restorer.stat.ValueNodeCnt,
		"embedded node", restorer.stat.EmbeddedNodeCnt)

	if batch.ValueSize() > 0 {
		if err := batch.Write(); err != nil {
			return err
		}
		batch.Reset()
	}

	log.Info("embedded node has been restored successfully", "elapsed", common.PrettyDuration(time.Since(start)))
	// TODO remove the following code, used to compare snapshot key num
	start = time.Now()
	count = 0
	var snapPrefix = [2][]byte{rawdb.SnapshotAccountPrefix, rawdb.SnapshotStoragePrefix}
	var SnapshotAccountKey int
	var SnapshotStorageKey int
	for _, prefix := range snapPrefix {
		it = restorer.db.NewIterator(prefix, nil)
		for it.Next() {
			key = it.Key()
			if bytes.Compare(prefix, rawdb.SnapshotAccountPrefix) == 0 {
				if len(key) != (len(rawdb.SnapshotAccountPrefix) + common.HashLength) {
					continue
				} else {
					SnapshotAccountKey++
				}
			}

			if bytes.Compare(prefix, rawdb.SnapshotStoragePrefix) == 0 {
				if len(key) != (len(rawdb.SnapshotStoragePrefix) + 2*common.HashLength) {
					continue
				} else {
					SnapshotStorageKey++
				}
			}

			count++
			if time.Since(logged) > 8*time.Second {
				log.Info("Checking snap state", "count", count, "elapsed", common.PrettyDuration(time.Since(start)))
				logged = time.Now()
			}
		}
		it.Release()
	}
	log.Info(" total snap key info ", "snap account", SnapshotAccountKey, "snap storage", SnapshotStorageKey)

	if uint64(SnapshotAccountKey+SnapshotStorageKey) != restorer.stat.EmbeddedNodeCnt+restorer.stat.ValueNodeCnt {
		log.Warn("compare not same", "snapshot total key", SnapshotAccountKey+SnapshotStorageKey,
			"value node key", restorer.stat.EmbeddedNodeCnt+restorer.stat.ValueNodeCnt)
	}
	return nil
}

func (restorer *EmbeddedNodeRestorer) DeleteStaleKv() error {
	headBlock := rawdb.ReadHeadBlock(restorer.db)
	if headBlock == nil {
		return errors.New("failed to load head block")
	}

	triedb := triedb.NewDatabase(restorer.db, triedb.HashDefaults)
	defer triedb.Close()

	snapconfig := snapshot.Config{
		CacheSize:  256,
		Recovery:   false,
		NoBuild:    true,
		AsyncBuild: false,
	}
	snaptree, err := snapshot.New(snapconfig, restorer.db, triedb, headBlock.Root(), 128, false)
	if err != nil {
		return err // The relevant snapshot(s) might not exist
	}

	// Retrieve the root node of persistent state.
	_, diskRoot := rawdb.ReadAccountTrieNode(restorer.db, nil)
	diskRoot = types.TrieRootHash(diskRoot)

	snapshot := snaptree.Snapshot(diskRoot)
	if snapshot == nil {
		return errors.New("snapshot empty")
	}
	var (
		it  ethdb.Iterator
		key []byte
	)

	it = restorer.db.NewIterator(rawdb.TrieNodeStoragePrefix, nil)
	for it.Next() {
		key = it.Key()
		if !rawdb.IsStorageTrieNode(key) {
			continue
		}
		// Get Account Hash
		accountHash := common.BytesToHash(key[1 : 1+common.HashLength])
		// Read Account Hash from snap
		simAcc, err := snapshot.Account(accountHash)
		if err != nil {
			return err
		}
		// if account.root == empty,  deleteRange(Account)
		if simAcc == nil || simAcc.Root == nil {
			if simAcc.Root == nil {
				err := rawdb.DeleteStorageTrie(restorer.db, accountHash)
			}
		}
	}
	it.Release()

}
