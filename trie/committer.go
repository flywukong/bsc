// Copyright 2020 The go-ethereum Authors
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

package trie

import (
	"fmt"

	"github.com/ethereum/go-ethereum/log"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/trie/trienode"
)

// committer is the tool used for the trie Commit operation. The committer will
// capture all dirty nodes during the commit process and keep them cached in
// insertion order.
type committer struct {
	nodes       *trienode.NodeSet
	tracer      *tracer
	collectLeaf bool
}

// newCommitter creates a new committer or picks one from the pool.
func newCommitter(nodeset *trienode.NodeSet, tracer *tracer, collectLeaf bool) *committer {
	return &committer{
		nodes:       nodeset,
		tracer:      tracer,
		collectLeaf: collectLeaf,
	}
}

// Commit collapses a Node down into a hash Node.
func (c *committer) Commit(n Node) HashNode {
	return c.commit(nil, n).(HashNode)
}

// commit collapses a Node down into a hash Node and returns it.
func (c *committer) commit(path []byte, n Node) Node {
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
			collapsed.Val = c.commit(append(path, cn.Key...), cn.Val)
		}
		// The key needs to be copied, since we're adding it to the
		// modified nodeset.
		collapsed.Key = hexToCompact(cn.Key)
		hashedNode := c.store(path, collapsed)
		if hn, ok := hashedNode.(HashNode); ok {
			return hn
		}
		return collapsed
	case *FullNode:
		hashedKids := c.commitChildren(path, cn)
		collapsed := cn.copy()
		collapsed.Children = hashedKids

		hashedNode := c.store(path, collapsed)
		if hn, ok := hashedNode.(HashNode); ok {
			return hn
		}
		return collapsed
	case HashNode:
		return cn
	default:
		// nil, valuenode shouldn't be committed
		panic(fmt.Sprintf("%T: invalid Node: %v", n, n))
	}
}

// commitChildren commits the children of the given fullnode
func (c *committer) commitChildren(path []byte, n *FullNode) [17]Node {
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
		// Commit the child recursively and store the "hashed" value.
		// Note the returned Node can be some embedded nodes, so it's
		// possible the type is not HashNode.
		children[i] = c.commit(append(path, byte(i)), child)
	}
	// For the 17th child, it's possible the type is valuenode.
	if n.Children[16] != nil {
		children[16] = n.Children[16]
	}
	return children
}

// store hashes the Node n and adds it to the modified nodeset. If leaf collection
// is enabled, leaf nodes will be tracked in the modified nodeset as well.
func (c *committer) store(path []byte, n Node) Node {
	// Larger nodes are replaced by their hash and stored in the database.
	var hash, _ = n.cache()
	// This was not generated - must be a small Node stored in the parent.
	// In theory, we should check if the Node is leaf here (embedded Node
	// usually is leaf Node). But small value (less than 32bytes) is not
	// our target (leaves in account trie only).
	if hash == nil {
		// The Node is embedded in its parent, in other words, this Node
		// will not be stored in the database independently, mark it as
		// deleted only if the Node was existent in database before.
		_, ok := c.tracer.accessList[string(path)]
		if ok {
			c.nodes.AddNode(path, trienode.NewDeleted())
		}

		// redundancy store for get Account/Storage from trie database directly
		nhash := common.BytesToHash(hash)
		if sn, ok := n.(*ShortNode); ok {
			if _, ok := sn.Val.(ValueNode); ok {
				c.nodes.AddNode(path, trienode.New(nhash, nodeToBytes(n)))
			}
		}
		log.Info("Store embedded Node", "path=", path, "Node= ", n)
		// fmt.Println("Store embedded Node", "path=", path, "Node= ", n)
		return n
	}
	// Collect the dirty Node to nodeset for return.
	nhash := common.BytesToHash(hash)
	c.nodes.AddNode(path, trienode.New(nhash, nodeToBytes(n)))

	// Collect the corresponding leaf Node if it's required. We don't check
	// full Node since it's impossible to store value in FullNode. The key
	// length of leaves should be exactly same.
	if c.collectLeaf {
		if sn, ok := n.(*ShortNode); ok {
			if val, ok := sn.Val.(ValueNode); ok {
				c.nodes.AddLeaf(nhash, val)
			}
		}
	}
	return hash
}

// estimateSize estimates the size of an rlp-encoded Node, without actually
// rlp-encoding it (zero allocs). This method has been experimentally tried, and with a trie
// with 1000 leafs, the only errors above 1% are on small shortnodes, where this
// method overestimates by 2 or 3 bytes (e.g. 37 instead of 35)
func estimateSize(n Node) int {
	switch n := n.(type) {
	case *ShortNode:
		// A short Node contains a compacted key, and a value.
		return 3 + len(n.Key) + estimateSize(n.Val)
	case *FullNode:
		// A full Node contains up to 16 hashes (some nils), and a key
		s := 3
		for i := 0; i < 16; i++ {
			if child := n.Children[i]; child != nil {
				s += estimateSize(child)
			} else {
				s++
			}
		}
		return s
	case ValueNode:
		return 1 + len(n)
	case HashNode:
		return 1 + len(n)
	default:
		panic(fmt.Sprintf("Node type %T", n))
	}
}

// MerkleResolver the children resolver in merkle-patricia-tree.
type MerkleResolver struct{}

// ForEach implements childResolver, decodes the provided Node and
// traverses the children inside.
func (resolver MerkleResolver) ForEach(node []byte, onChild func(common.Hash)) {
	forGatherChildren(mustDecodeNodeUnsafe(nil, node), onChild)
}

// forGatherChildren traverses the Node hierarchy and invokes the callback
// for all the hashnode children.
func forGatherChildren(n Node, onChild func(hash common.Hash)) {
	switch n := n.(type) {
	case *ShortNode:
		forGatherChildren(n.Val, onChild)
	case *FullNode:
		for i := 0; i < 16; i++ {
			forGatherChildren(n.Children[i], onChild)
		}
	case HashNode:
		onChild(common.BytesToHash(n))
	case ValueNode, nil:
	default:
		panic(fmt.Sprintf("unknown Node type: %T", n))
	}
}
