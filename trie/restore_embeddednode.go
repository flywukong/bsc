package trie

import (
	"bytes"
	"errors"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
	"golang.org/x/crypto/sha3"
)

const ExpectLeafNodeLen = 32

type EmbeddedNodeRestorer struct {
	db     ethdb.Database
	newDB  ethdb.Database
	Triedb Database
	stat   *dbNodeStat
}

type dbNodeStat struct {
	ShortNodeCnt    uint64
	ValueNodeCnt    uint64
	FullNodeCnt     uint64
	EmbeddedNodeCnt uint64
}

func NewEmbeddedNodeRestorer(chaindb ethdb.Database) *EmbeddedNodeRestorer {
	return &EmbeddedNodeRestorer{
		db: chaindb,
		//newDB: targetDB,
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

func (restorer *EmbeddedNodeRestorer) Run2() error {
	_, diskRoot := rawdb.ReadAccountTrieNode(restorer.db, nil)
	diskRoot = types.TrieRootHash(diskRoot)
	log.Info("disk root info", "hash", diskRoot)

	t, err := NewStateTrie(StateTrieID(diskRoot), restorer.Triedb)
	if err != nil {
		log.Error("Failed to open trie", "root", diskRoot, "err", err)
		return err
	}
	var (
		nodes          int
		accounts       int
		slots          int
		lastReport     time.Time
		start          = time.Now()
		emptyBlobNodes int
		CA_account     int
		embeddedCount  = 0
		//keccakStateHasher   = crypto.NewKeccakState()
		//got                 = make([]byte, 32)
		// batch               = restorer.newDB.NewBatch()
		invalidNode         = 0
		embeddedNode        int
		storageEmbeddedNode int
		storageEmptyHash    int
	)

	accIter, err := t.NodeIterator(nil)
	if err != nil {
		log.Error("Failed to open iterator", "root", diskRoot, "err", err)
		return err
	}

	for accIter.Next(true) {
		nodes += 1

		// write the node into db
		accKey := accountTrieNodeKey(accIter.Path())
		accValue := accIter.NodeBlob()
		if accValue == nil {
			log.Warn("trie node(account) with node blob empty")
			embeddedNode++
		}
		compareValue, err := restorer.db.Get(accKey)
		if err == nil && bytes.Compare(compareValue, accValue) == 0 {
			log.Info("compare value in db same", "key", common.Bytes2Hex(accKey))
		} else {
			log.Info("compare value in db not same", "err", err.Error(),
				"key", common.Bytes2Hex(accKey), "node blob", common.Bytes2Hex(accValue), ""+
					"db value", common.Bytes2Hex(compareValue))
		}

		// If it's a leaf node, yes we are touching an account,
		// dig into the storage trie further.
		if accIter.Leaf() {
			accounts += 1
			var acc types.StateAccount
			if err := rlp.DecodeBytes(accIter.LeafBlob(), &acc); err != nil {
				log.Error("Invalid account encountered during traversal", "err", err)
				return errors.New("invalid account")
			}

			// if it is a CA account , iterator the storage trie to find embedded node
			if acc.Root != types.EmptyRootHash {
				ownerHash := common.BytesToHash(accIter.LeafKey())
				id := StorageTrieID(diskRoot, ownerHash, acc.Root)
				storageTrie, err := NewStateTrie(id, restorer.Triedb)
				if err != nil {
					log.Error("Failed to open storage trie", "root", acc.Root, "err", err)
					return errors.New("missing storage trie")
				}

				storageIter, err := storageTrie.NodeIterator(nil)
				if err != nil {
					log.Error("Failed to open storage iterator", "root", acc.Root, "err", err)
					return err
				}
				// iterator the storage trie
				for storageIter.Next(true) {
					nodes += 1
					nodeblob := storageIter.NodeBlob()
					if nodeblob == nil {
						log.Warn("trie node(storage) with node blob empty")
						embeddedNode++
						continue
					}

					h := rawdb.NewSha256Hasher()
					hash := h.Hash(nodeblob)
					h.Release()

					key := storageTrieNodeKey(ownerHash, storageIter.Path())

					compareValue2, err := restorer.db.Get(key)
					if err == nil && bytes.Compare(compareValue2, nodeblob) == 0 {
						log.Info("compare value in db same", "key", common.Bytes2Hex(key))
					} else {
						log.Info("compare value in db not same", "err", err.Error(),
							"key", common.Bytes2Hex(key), "node blob", common.Bytes2Hex(nodeblob), ""+
								"db value", common.Bytes2Hex(compareValue2))
					}

					// write storage  node
					//if err := batch.Put(key, nodeblob); err != nil {
					//	return err
					//}
					// check if is full short node inside full node
					shortnodeList, err := checkIfContainShortNode(hash.Bytes(), key, nodeblob, restorer.stat)
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
								newKey := append(key, byte(snode.Idx))
								log.Info("embedded storage shortNode info", "trie key", common.Bytes2Hex(key),
									"fullNode path", common.Bytes2Hex(fullNodePath),
									"new node key", common.Bytes2Hex(newKey), "new node value", common.Bytes2Hex(snode.NodeBytes))
								// batch write
								//	if err := batch.Put(newKey, snode.NodeBytes); err != nil {
								//		return err
							}

						}
					}
					// Bump the counter if it's leaf node.
					if storageIter.Leaf() {
						CA_account += 1
					}

					if time.Since(lastReport) > time.Second*3 {
						log.Info("Traversing state", "nodes", nodes, "accounts", accounts, "CA account", CA_account,
							"embedded", embeddedCount, "storage embedded node", storageEmbeddedNode,
							"invalid", invalidNode, "empty hash", storageEmptyHash, "empty blob", emptyBlobNodes, "elapsed",
							common.PrettyDuration(time.Since(start)))
						lastReport = time.Now()
					}
				}

				if storageIter.Error() != nil {
					log.Error("Failed to traverse storage trie", "root", acc.Root, "err", storageIter.Error())
					return storageIter.Error()
				}
			}

			if time.Since(lastReport) > time.Second*8 {
				log.Info("Traversing state", "nodes", nodes, "accounts", accounts, "CA account", CA_account,
					"embedded", embeddedCount, "storage embedded node", storageEmbeddedNode,
					"invalid", invalidNode, "empty hash", storageEmptyHash, "empty blob", emptyBlobNodes, "elapsed",
					common.PrettyDuration(time.Since(start)))
				lastReport = time.Now()
			}
		}
	}
	if accIter.Error() != nil {
		log.Error("Failed to traverse state trie", "root", diskRoot, "err", accIter.Error())
		return accIter.Error()
	}

	log.Info("embedded node info", "storage embedded node", storageEmbeddedNode, "storage empty", storageEmptyHash)

	log.Info("State is complete", "nodes", nodes, "accounts", accounts, "slots", slots, "elapsed", common.PrettyDuration(time.Since(start)))
	return nil
}

// accountTrieNodeKey = TrieNodeAccountPrefix + nodePath.
func accountTrieNodeKey(path []byte) []byte {
	return append(rawdb.TrieNodeAccountPrefix, path...)
}

// storageTrieNodeKey = TrieNodeStoragePrefix + accountHash + nodePath.
func storageTrieNodeKey(accountHash common.Hash, path []byte) []byte {
	buf := make([]byte, len(rawdb.TrieNodeStoragePrefix)+common.HashLength+len(path))
	n := copy(buf, rawdb.TrieNodeStoragePrefix)
	n += copy(buf[n:], accountHash.Bytes())
	copy(buf[n:], path)
	return buf
}
