package trie

import (
	"bytes"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie/trienode"
)

type Hbss2Pbss struct {
	trie            *Trie // traverse trie
	db              Database
	blocknum        uint64
	root            Node // root of triedb
	stateRootHash   common.Hash
	concurrentQueue chan struct{}
	totalNum        uint64
	wg              sync.WaitGroup
}

const (
	DEFAULT_TRIEDBCACHE_SIZE = 1024 * 1024 * 1024
)

// NewHbss2Pbss return a hash2Path obj
func NewHbss2Pbss(tr *Trie, db Database, stateRootHash common.Hash, blocknum uint64, jobnum uint64) (*Hbss2Pbss, error) {
	if tr == nil {
		return nil, errors.New("trie is nil")
	}

	if tr.root == nil {
		return nil, errors.New("trie root is nil")
	}

	ins := &Hbss2Pbss{
		trie:            tr,
		blocknum:        blocknum,
		db:              db,
		stateRootHash:   stateRootHash,
		root:            tr.root,
		concurrentQueue: make(chan struct{}, jobnum),
		wg:              sync.WaitGroup{},
	}

	return ins, nil
}

func (t *Trie) resloveWithoutTrack(n Node, prefix []byte) (Node, error) {
	if n, ok := n.(HashNode); ok {
		blob, err := t.reader.node(prefix, common.BytesToHash(n))
		if err != nil {
			return nil, err
		}
		return mustDecodeNode(n, blob), nil
	}
	return n, nil
}

func (h2p *Hbss2Pbss) writeNode(pathKey []byte, n *trienode.Node, owner common.Hash) {
	if owner == (common.Hash{}) {
		rawdb.WriteAccountTrieNode(h2p.db.DiskDB(), pathKey, n.Blob)
		log.Debug("WriteNodes account Node, ", "path: ", common.Bytes2Hex(pathKey), "Hash: ", n.Hash, "BlobHash: ", crypto.Keccak256Hash(n.Blob))
	} else {
		rawdb.WriteStorageTrieNode(h2p.db.DiskDB(), owner, pathKey, n.Blob)
		log.Debug("WriteNodes storage Node, ", "path: ", common.Bytes2Hex(pathKey), "owner: ", owner.String(), "Hash: ", n.Hash, "BlobHash: ", crypto.Keccak256Hash(n.Blob))
	}
}

// Run statistics, external call
func (h2p *Hbss2Pbss) Run() {
	log.Debug("Find Account Trie Tree, rootHash: ", h2p.trie.Hash().String(), "BlockNum: ", h2p.blocknum)

	h2p.ConcurrentTraversal(h2p.trie, h2p.root, []byte{})
	h2p.wg.Wait()

	log.Info("Total", "complete", h2p.totalNum, "go routines Num", runtime.NumGoroutine, "h2p concurrentQueue", len(h2p.concurrentQueue))

	rawdb.WritePersistentStateID(h2p.db.DiskDB(), h2p.blocknum)
	rawdb.WriteStateID(h2p.db.DiskDB(), h2p.stateRootHash, h2p.blocknum)
}

func (h2p *Hbss2Pbss) SubConcurrentTraversal(theTrie *Trie, theNode Node, path []byte) {
	h2p.concurrentQueue <- struct{}{}
	h2p.ConcurrentTraversal(theTrie, theNode, path)
	<-h2p.concurrentQueue
	h2p.wg.Done()
}

func (h2p *Hbss2Pbss) ConcurrentTraversal(theTrie *Trie, theNode Node, path []byte) {
	total_num := uint64(0)
	// nil Node
	if theNode == nil {
		return
	}

	switch current := (theNode).(type) {
	case *ShortNode:
		collapsed := current.copy()
		collapsed.Key = hexToCompact(current.Key)
		var hash, _ = current.cache()
		h2p.writeNode(path, trienode.New(common.BytesToHash(hash), nodeToBytes(collapsed)), theTrie.owner)

		h2p.ConcurrentTraversal(theTrie, current.Val, append(path, current.Key...))

	case *FullNode:
		// copy from trie/Committer (*committer).commit
		collapsed := current.copy()
		var hash, _ = collapsed.cache()
		collapsed.Children = h2p.commitChildren(path, current)

		nodebytes := nodeToBytes(collapsed)
		if common.BytesToHash(hash) != common.BytesToHash(crypto.Keccak256(nodebytes)) {
			log.Error("Hash is inconsistent, hash: ", common.BytesToHash(hash), "Node hash: ", common.BytesToHash(crypto.Keccak256(nodebytes)), "Node: ", collapsed.fstring(""))
			panic("hash inconsistent.")
		}

		h2p.writeNode(path, trienode.New(common.BytesToHash(hash), nodeToBytes(collapsed)), theTrie.owner)

		for idx, child := range current.Children {
			if child == nil {
				continue
			}
			childPath := append(path, byte(idx))
			if len(h2p.concurrentQueue)*2 < cap(h2p.concurrentQueue) {
				h2p.wg.Add(1)
				dst := make([]byte, len(childPath))
				copy(dst, childPath)
				go h2p.SubConcurrentTraversal(theTrie, child, dst)
			} else {
				h2p.ConcurrentTraversal(theTrie, child, childPath)
			}
		}
	case HashNode:
		n, err := theTrie.resloveWithoutTrack(current, path)
		if err != nil {
			log.Error("Resolve HashNode", "error", err, "TrieRoot", theTrie.Hash(), "Path", path)
			return
		}
		h2p.ConcurrentTraversal(theTrie, n, path)
		total_num = atomic.AddUint64(&h2p.totalNum, 1)
		if total_num%100000 == 0 {
			log.Info("Converting ", "Complete progress", total_num, "go routines Num", runtime.NumGoroutine(), "h2p concurrentQueue", len(h2p.concurrentQueue))
		}
		return
	case ValueNode:
		if !hasTerm(path) {
			log.Info("ValueNode miss path term", "path", common.Bytes2Hex(path))
			break
		}
		var account types.StateAccount
		if err := rlp.Decode(bytes.NewReader(current), &account); err != nil {
			// log.Info("Rlp decode account failed.", "err", err)
			break
		}
		if account.Root == (common.Hash{}) || account.Root == types.EmptyRootHash {
			// log.Info("Not a storage trie.", "account", common.BytesToHash(path).String())
			break
		}

		ownerAddress := common.BytesToHash(hexToCompact(path))
		tr, err := New(StorageTrieID(h2p.stateRootHash, ownerAddress, account.Root), h2p.db)
		if err != nil {
			log.Error("New Storage trie error", "err", err, "root", account.Root.String(), "owner", ownerAddress.String())
			break
		}
		log.Debug("Find Contract Trie Tree", "rootHash: ", tr.Hash().String(), "")
		h2p.wg.Add(1)
		go h2p.SubConcurrentTraversal(tr, tr.root, []byte{})
	default:
		panic(errors.New("Invalid Node type to traverse."))
	}
}

// copy from trie/Commiter (*committer).commit
func (h2p *Hbss2Pbss) commitChildren(path []byte, n *FullNode) [17]Node {
	var children [17]Node
	for i := 0; i < 16; i++ {
		child := n.Children[i]
		if child == nil {
			continue
		}
		// If it's the hashed child, save the hash value directly.
		// Note: it's impossible that the child in range [0, 15]
		// is a ValueNode.
		if hn, ok := child.(HashNode); ok {
			children[i] = hn
			continue
		}

		children[i] = h2p.commit(append(path, byte(i)), child)
	}
	// For the 17th child, it's possible the type is valuenode.
	if n.Children[16] != nil {
		children[16] = n.Children[16]
	}
	return children
}

// commit collapses a Node down into a hash Node and returns it.
func (h2p *Hbss2Pbss) commit(path []byte, n Node) Node {
	// if this path is clean, use available cached data
	hash, dirty := n.cache()
	if hash != nil && !dirty {
		return hash
	}
	// Commit children, then parent, and remove the dirty flag.
	switch cn := n.(type) {
	case *ShortNode:
		// Commit child
		collapsed := cn.copy()

		// If the child is FullNode, recursively commit,
		// otherwise it can only be HashNode or ValueNode.
		if _, ok := cn.Val.(*FullNode); ok {
			collapsed.Val = h2p.commit(append(path, cn.Key...), cn.Val)
		}
		// The key needs to be copied, since we're adding it to the
		// modified nodeset.
		collapsed.Key = hexToCompact(cn.Key)
		return collapsed
	case *FullNode:
		hashedKids := h2p.commitChildren(path, cn)
		collapsed := cn.copy()
		collapsed.Children = hashedKids

		return collapsed
	case HashNode:
		return cn
	default:
		// nil, valuenode shouldn't be committed
		panic(fmt.Sprintf("%T: invalid Node: %v", n, n))
	}
}
