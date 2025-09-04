// Copyright 2018 The go-ethereum Authors
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

package rawdb

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/ethdb/memorydb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/olekukonko/tablewriter"
)

// Helper function to get minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// freezerdb is a database wrapper that enables ancient chain segment freezing.
type freezerdb struct {
	ethdb.KeyValueStore
	ethdb.AncientStore

	readOnly    bool
	ancientRoot string

	ethdb.AncientFreezer
	stateStore   ethdb.Database
	snapStore    ethdb.KeyValueStore
	txIndexStore ethdb.KeyValueStore
}

func (frdb *freezerdb) StateStoreReader() ethdb.Reader {
	if frdb.stateStore == nil {
		return frdb
	}
	return frdb.stateStore
}

func (frdb *freezerdb) IndexStoreReader() ethdb.KeyValueReader {
	if frdb.txIndexStore != nil {
		return frdb.txIndexStore
	}
	return frdb.KeyValueStore
}

// AncientDatadir returns the path of root ancient directory.
func (frdb *freezerdb) AncientDatadir() (string, error) {
	return frdb.ancientRoot, nil
}

// Close implements io.Closer, closing both the fast key-value store as well as
// the slow ancient tables.
func (frdb *freezerdb) Close() error {
	var errs []error
	if err := frdb.AncientStore.Close(); err != nil {
		errs = append(errs, err)
	}
	if err := frdb.KeyValueStore.Close(); err != nil {
		errs = append(errs, err)
	}
	if frdb.HasSeparateStateStore() {
		if err := frdb.GetStateStore().Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if frdb.HasSeparateSnapStore() {
		if err := frdb.GetSnapStore().Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if frdb.HasSeparateTxIndexStore() {
		if err := frdb.GetTxIndexStore().Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) != 0 {
		return fmt.Errorf("%v", errs)
	}
	return nil
}

func (frdb *freezerdb) SetStateStore(state ethdb.Database) {
	if frdb.stateStore != nil {
		frdb.stateStore.Close()
	}
	frdb.stateStore = state
}

func (frdb *freezerdb) GetStateStore() ethdb.Database {
	if frdb.stateStore != nil {
		return frdb.stateStore
	}
	return frdb
}

func (frdb *freezerdb) HasSeparateStateStore() bool {
	return frdb.stateStore != nil
}

func (frdb *freezerdb) SetSnapStore(snapStore ethdb.KeyValueStore) {
	if frdb.snapStore != nil {
		frdb.snapStore.Close()
	}
	frdb.snapStore = snapStore
}

func (frdb *freezerdb) GetSnapStore() ethdb.KeyValueStore {
	if frdb.snapStore != nil {
		return frdb.snapStore
	}
	return frdb.KeyValueStore
}

func (frdb *freezerdb) HasSeparateSnapStore() bool {
	return frdb.snapStore != nil
}

func (frdb *freezerdb) SetTxIndexStore(store ethdb.KeyValueStore) {
	if frdb.txIndexStore != nil {
		frdb.txIndexStore.Close()
	}
	frdb.txIndexStore = store
}

func (frdb *freezerdb) GetTxIndexStore() ethdb.KeyValueStore {
	if frdb.txIndexStore != nil {
		return frdb.txIndexStore
	}
	return frdb.KeyValueStore
}

func (frdb *freezerdb) HasSeparateTxIndexStore() bool {
	return frdb.txIndexStore != nil
}

// Freeze is a helper method used for external testing to trigger and block until
// a freeze cycle completes, without having to sleep for a minute to trigger the
// automatic background run.
func (frdb *freezerdb) Freeze(threshold uint64) error {
	if frdb.readOnly {
		return errReadOnly
	}
	// Set the freezer threshold to a temporary value
	defer func(old uint64) {
		frdb.AncientStore.(*chainFreezer).threshold.Store(old)
	}(frdb.AncientStore.(*chainFreezer).threshold.Load())
	frdb.AncientStore.(*chainFreezer).threshold.Store(threshold)
	// Trigger a freeze cycle and block until it's done
	trigger := make(chan struct{}, 1)
	frdb.AncientStore.(*chainFreezer).trigger <- trigger
	<-trigger
	return nil
}

func (frdb *freezerdb) SetupFreezerEnv(env *ethdb.FreezerEnv, blockHistory uint64) error {
	return frdb.AncientFreezer.SetupFreezerEnv(env, blockHistory)
}

// nofreezedb is a database wrapper that disables freezer data retrievals.
type nofreezedb struct {
	ethdb.KeyValueStore
	stateStore   ethdb.Database
	snapStore    ethdb.KeyValueStore
	txIndexStore ethdb.KeyValueStore
}

// HasAncient returns an error as we don't have a backing chain freezer.
func (db *nofreezedb) HasAncient(kind string, number uint64) (bool, error) {
	return false, errNotSupported
}

// Ancient returns an error as we don't have a backing chain freezer.
func (db *nofreezedb) Ancient(kind string, number uint64) ([]byte, error) {
	return nil, errNotSupported
}

// AncientRange returns an error as we don't have a backing chain freezer.
func (db *nofreezedb) AncientRange(kind string, start, max, maxByteSize uint64) ([][]byte, error) {
	return nil, errNotSupported
}

// Ancients returns an error as we don't have a backing chain freezer.
func (db *nofreezedb) Ancients() (uint64, error) {
	return 0, errNotSupported
}

// ItemAmountInAncient returns an error as we don't have a backing chain freezer.
func (db *nofreezedb) ItemAmountInAncient() (uint64, error) {
	return 0, errNotSupported
}

// Tail returns an error as we don't have a backing chain freezer.
func (db *nofreezedb) Tail() (uint64, error) {
	return 0, errNotSupported
}

// AncientSize returns an error as we don't have a backing chain freezer.
func (db *nofreezedb) AncientSize(kind string) (uint64, error) {
	return 0, errNotSupported
}

// ModifyAncients is not supported.
func (db *nofreezedb) ModifyAncients(func(ethdb.AncientWriteOp) error) (int64, error) {
	return 0, errNotSupported
}

// TruncateHead returns an error as we don't have a backing chain freezer.
func (db *nofreezedb) TruncateHead(items uint64) (uint64, error) {
	return 0, errNotSupported
}

// TruncateTail returns an error as we don't have a backing chain freezer.
func (db *nofreezedb) TruncateTail(items uint64) (uint64, error) {
	return 0, errNotSupported
}

// TruncateTableTail will truncate certain table to new tail
func (db *nofreezedb) TruncateTableTail(kind string, tail uint64) (uint64, error) {
	return 0, errNotSupported
}

// ResetTable will reset certain table with new start point
func (db *nofreezedb) ResetTable(kind string, startAt uint64, onlyEmpty bool) error {
	return errNotSupported
}

// SyncAncient returns an error as we don't have a backing chain freezer.
func (db *nofreezedb) SyncAncient() error {
	return errNotSupported
}

func (db *nofreezedb) SetStateStore(state ethdb.Database) {
	db.stateStore = state
}

func (db *nofreezedb) GetStateStore() ethdb.Database {
	if db.stateStore != nil {
		return db.stateStore
	}
	return db
}

func (db *nofreezedb) HasSeparateStateStore() bool {
	return db.stateStore != nil
}

func (db *nofreezedb) StateStoreReader() ethdb.Reader {
	if db.stateStore != nil {
		return db.stateStore
	}
	return db
}

func (db *nofreezedb) IndexStoreReader() ethdb.KeyValueReader {
	if db.txIndexStore != nil {
		return db.txIndexStore
	}
	return db.KeyValueStore
}

func (db *nofreezedb) SetSnapStore(snapStore ethdb.KeyValueStore) {
	if db.snapStore != nil {
		db.snapStore.Close()
	}
	db.snapStore = snapStore
}

func (db *nofreezedb) GetSnapStore() ethdb.KeyValueStore {
	if db.snapStore != nil {
		return db.snapStore
	}
	return db.KeyValueStore
}

func (db *nofreezedb) HasSeparateSnapStore() bool {
	return db.snapStore != nil
}

func (db *nofreezedb) SetTxIndexStore(store ethdb.KeyValueStore) {
	if db.txIndexStore != nil {
		db.txIndexStore.Close()
	}
	db.txIndexStore = store
}

func (db *nofreezedb) GetTxIndexStore() ethdb.KeyValueStore {
	if db.txIndexStore != nil {
		return db.txIndexStore
	}
	return db.KeyValueStore
}

func (db *nofreezedb) HasSeparateTxIndexStore() bool {
	return db.txIndexStore != nil
}

func (db *nofreezedb) ReadAncients(fn func(reader ethdb.AncientReaderOp) error) (err error) {
	// Unlike other ancient-related methods, this method does not return
	// errNotSupported when invoked.
	// The reason for this is that the caller might want to do several things:
	// 1. Check if something is in the freezer,
	// 2. If not, check leveldb.
	//
	// This will work, since the ancient-checks inside 'fn' will return errors,
	// and the leveldb work will continue.
	//
	// If we instead were to return errNotSupported here, then the caller would
	// have to explicitly check for that, having an extra clause to do the
	// non-ancient operations.
	return fn(db)
}

func (db *nofreezedb) AncientOffSet() uint64 {
	return 0
}

// AncientDatadir returns an error as we don't have a backing chain freezer.
func (db *nofreezedb) AncientDatadir() (string, error) {
	return "", errNotSupported
}

func (db *nofreezedb) SetupFreezerEnv(env *ethdb.FreezerEnv, blockHistory uint64) error {
	return nil
}

// NewDatabase creates a high level database on top of a given key-value data
// store without a freezer moving immutable chain segments into cold storage.
func NewDatabase(db ethdb.KeyValueStore) ethdb.Database {
	return &nofreezedb{KeyValueStore: db}
}

type emptyfreezedb struct {
	ethdb.KeyValueStore
}

// HasAncient returns nil for pruned db that we don't have a backing chain freezer.
func (db *emptyfreezedb) HasAncient(kind string, number uint64) (bool, error) {
	return false, nil
}

// Ancient returns nil for pruned db that we don't have a backing chain freezer.
func (db *emptyfreezedb) Ancient(kind string, number uint64) ([]byte, error) {
	return nil, nil
}

// AncientRange returns nil for pruned db that we don't have a backing chain freezer.
func (db *emptyfreezedb) AncientRange(kind string, start, max, maxByteSize uint64) ([][]byte, error) {
	return nil, nil
}

// Ancients returns nil for pruned db that we don't have a backing chain freezer.
func (db *emptyfreezedb) Ancients() (uint64, error) {
	return 0, nil
}

// ItemAmountInAncient returns nil for pruned db that we don't have a backing chain freezer.
func (db *emptyfreezedb) ItemAmountInAncient() (uint64, error) {
	return 0, nil
}

// Tail returns nil for pruned db that we don't have a backing chain freezer.
func (db *emptyfreezedb) Tail() (uint64, error) {
	return 0, nil
}

// AncientSize returns nil for pruned db that we don't have a backing chain freezer.
func (db *emptyfreezedb) AncientSize(kind string) (uint64, error) {
	return 0, nil
}

// ModifyAncients returns nil for pruned db that we don't have a backing chain freezer.
func (db *emptyfreezedb) ModifyAncients(func(ethdb.AncientWriteOp) error) (int64, error) {
	return 0, nil
}

// TruncateHead returns nil for pruned db that we don't have a backing chain freezer.
func (db *emptyfreezedb) TruncateHead(items uint64) (uint64, error) {
	return 0, nil
}

// TruncateTail returns nil for pruned db that we don't have a backing chain freezer.
func (db *emptyfreezedb) TruncateTail(items uint64) (uint64, error) {
	return 0, nil
}

// TruncateTableTail returns nil for pruned db that we don't have a backing chain freezer.
func (db *emptyfreezedb) TruncateTableTail(kind string, tail uint64) (uint64, error) {
	return 0, nil
}

// ResetTable returns nil for pruned db that we don't have a backing chain freezer.
func (db *emptyfreezedb) ResetTable(kind string, startAt uint64, onlyEmpty bool) error {
	return nil
}

// SyncAncient returns nil for pruned db that we don't have a backing chain freezer.
func (db *emptyfreezedb) SyncAncient() error {
	return nil
}

func (db *emptyfreezedb) GetStateStore() ethdb.Database              { return db }
func (db *emptyfreezedb) SetStateStore(state ethdb.Database)         {}
func (db *emptyfreezedb) StateStoreReader() ethdb.Reader             { return db }
func (db *emptyfreezedb) HasSeparateStateStore() bool                { return false }
func (db *emptyfreezedb) GetSnapStore() ethdb.KeyValueStore          { return db.KeyValueStore }
func (db *emptyfreezedb) SetSnapStore(snapStore ethdb.KeyValueStore) {}
func (db *emptyfreezedb) HasSeparateSnapStore() bool                 { return false }
func (db *emptyfreezedb) GetTxIndexStore() ethdb.KeyValueStore       { return db.KeyValueStore }
func (db *emptyfreezedb) SetTxIndexStore(store ethdb.KeyValueStore)  {}
func (db *emptyfreezedb) HasSeparateTxIndexStore() bool              { return false }
func (db *emptyfreezedb) IndexStoreReader() ethdb.KeyValueReader     { return db.KeyValueStore }
func (db *emptyfreezedb) ReadAncients(fn func(reader ethdb.AncientReaderOp) error) (err error) {
	return nil
}
func (db *emptyfreezedb) AncientOffSet() uint64 { return 0 }

// AncientDatadir returns nil for pruned db that we don't have a backing chain freezer.
func (db *emptyfreezedb) AncientDatadir() (string, error) {
	return "", nil
}
func (db *emptyfreezedb) SetupFreezerEnv(env *ethdb.FreezerEnv, blockHistory uint64) error {
	return nil
}

// NewEmptyFreezeDB is used for CLI such as `geth db inspect` in pruned db that we don't
// have a backing chain freezer.
// WARNING: it must be only used in the above case.
func NewEmptyFreezeDB(db ethdb.KeyValueStore) ethdb.Database {
	return &emptyfreezedb{KeyValueStore: db}
}

// NewFreezerDb only create a freezer without statedb.
func NewFreezerDb(db ethdb.KeyValueStore, frz, namespace string, readonly bool, newOffSet uint64) (*Freezer, error) {
	// Create the idle freezer instance, this operation should be atomic to avoid mismatch between offset and acientDB.
	frdb, err := NewFreezer(frz, namespace, readonly, freezerTableSize, chainFreezerNoSnappy)
	if err != nil {
		return nil, err
	}
	return frdb, nil
}

// resolveChainFreezerDir is a helper function which resolves the absolute path
// of chain freezer by considering backward compatibility.
//
// rules:
// 1. in path mode, block data is stored in chain dir and state data is in state dir.
// 2. in hash mode, block data is stored in chain dir or ancient dir(before big merge), no state dir.
func resolveChainFreezerDir(ancient string) string {
	// Check if the chain freezer is already present in the specified
	// sub folder, if not then two possibilities:
	// - chain freezer is not initialized
	// - chain freezer exists in legacy location (root ancient folder)
	chain := filepath.Join(ancient, ChainFreezerName)
	state := filepath.Join(ancient, MerkleStateFreezerName)
	if common.FileExist(chain) {
		return chain
	}
	if common.FileExist(state) {
		return chain
	}
	if common.FileExist(ancient) {
		log.Info("Found legacy ancient chain path", "location", ancient)
		chain = ancient
	}
	return chain
}

// NewDatabaseWithFreezer creates a high level database on top of a given key-
// value data store with a freezer moving immutable chain segments into cold
// storage. The passed ancient indicates the path of root ancient directory
// where the chain freezer can be opened.
func NewDatabaseWithFreezer(db ethdb.KeyValueStore, ancient string, namespace string, readonly, disableFreeze, multiDatabase bool) (ethdb.Database, error) {
	// Create the idle freezer instance. If the given ancient directory is empty,
	// in-memory chain freezer is used (e.g. dev mode); otherwise the regular
	// file-based freezer is created.
	chainFreezerDir := ancient
	if chainFreezerDir != "" {
		chainFreezerDir = resolveChainFreezerDir(chainFreezerDir)
	}

	// if there has legacy offset, try to clean & reset the freezer metadata
	if legacyOffset := ReadLegacyOffset(db); legacyOffset > 0 {
		log.Info("Found legacy offset in freezerDB, will reset freezer meta", "offset", legacyOffset)
		if err := resetFreezerMeta(chainFreezerDir, namespace, legacyOffset); err != nil {
			return nil, err
		}
		CleanLegacyOffset(db)
	}

	// Create the idle freezer instance
	frdb, err := newChainFreezer(chainFreezerDir, namespace, readonly, multiDatabase)

	// We are creating the freezerdb here because the validation logic for db and freezer below requires certain interfaces
	// that need a database type. Therefore, we are pre-creating it for subsequent use.
	freezerDb := &freezerdb{
		ancientRoot:    ancient,
		KeyValueStore:  db,
		AncientStore:   frdb,
		AncientFreezer: frdb,
	}
	if err != nil {
		printChainMetadata(freezerDb)
		return nil, err
	}

	// Since the freezer can be stored separately from the user's key-value database,
	// there's a fairly high probability that the user requests invalid combinations
	// of the freezer and database. Ensure that we don't shoot ourselves in the foot
	// by serving up conflicting data, leading to both datastores getting corrupted.
	//
	//   - If both the freezer and key-value store are empty (no genesis), we just
	//     initialized a new empty freezer, so everything's fine.
	//   - If the key-value store is empty, but the freezer is not, we need to make
	//     sure the user's genesis matches the freezer. That will be checked in the
	//     blockchain, since we don't have the genesis block here (nor should we at
	//     this point care, the key-value/freezer combo is valid).
	//   - If neither the key-value store nor the freezer is empty, cross validate
	//     the genesis hashes to make sure they are compatible. If they are, also
	//     ensure that there's no gap between the freezer and subsequently leveldb.
	//   - If the key-value store is not empty, but the freezer is, we might just be
	//     upgrading to the freezer release, or we might have had a small chain and
	//     not frozen anything yet. Ensure that no blocks are missing yet from the
	//     key-value store, since that would mean we already had an old freezer.

	// If the genesis hash is empty, we have a new key-value store, so nothing to
	// validate in this method. If, however, the genesis hash is not nil, compare
	// it to the freezer content.
	// Only to check the followings when offset/ancientTail equal to 0, otherwise the block number
	// in ancientdb did not start with 0, no genesis block in ancientdb as well.
	ancientTail, err := frdb.Tail()
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve Tail from ancient %v", err)
	}
	if kvgenesis, _ := db.Get(headerHashKey(0)); ancientTail == 0 && len(kvgenesis) > 0 {
		if frozen, _ := frdb.Ancients(); frozen > 0 {
			// If the freezer already contains something, ensure that the genesis blocks
			// match, otherwise we might mix up freezers across chains and destroy both
			// the freezer and the key-value store.
			frgenesis, err := frdb.Ancient(ChainFreezerHashTable, 0)
			if err != nil {
				printChainMetadata(freezerDb)
				return nil, fmt.Errorf("failed to retrieve genesis from ancient %v", err)
			} else if !bytes.Equal(kvgenesis, frgenesis) {
				printChainMetadata(freezerDb)
				return nil, fmt.Errorf("genesis mismatch: %#x (leveldb) != %#x (ancients)", kvgenesis, frgenesis)
			}
			// Key-value store and freezer belong to the same network. Ensure that they
			// are contiguous, otherwise we might end up with a non-functional freezer.
			if kvhash, _ := db.Get(headerHashKey(frozen)); len(kvhash) == 0 {
				// Subsequent header after the freezer limit is missing from the database.
				// Reject startup if the database has a more recent head.
				if head := *ReadHeaderNumber(freezerDb, ReadHeadHeaderHash(freezerDb)); head > frozen-1 {
					// Find the smallest block stored in the key-value store
					// in range of [frozen, head]
					var number uint64
					for number = frozen; number <= head; number++ {
						if present, _ := db.Has(headerHashKey(number)); present {
							break
						}
					}
					// We are about to exit on error. Print database metadata before exiting
					printChainMetadata(freezerDb)
					return nil, fmt.Errorf("gap in the chain between ancients [0 - #%d] and leveldb [#%d - #%d] ",
						frozen-1, number, head)
				}
				// Database contains only older data than the freezer, this happens if the
				// state was wiped and reinited from an existing freezer.
			}
			// Otherwise, key-value store continues where the freezer left off, all is fine.
			// We might have duplicate blocks (crash after freezer write but before key-value
			// store deletion, but that's fine).
		} else {
			// If the freezer is empty, ensure nothing was moved yet from the key-value
			// store, otherwise we'll end up missing data. We check block #1 to decide
			// if we froze anything previously or not, but do take care of databases with
			// only the genesis block.
			if ReadHeadHeaderHash(freezerDb) != common.BytesToHash(kvgenesis) {
				// Key-value store contains more data than the genesis block, make sure we
				// didn't freeze anything yet.
				if kvblob, _ := db.Get(headerHashKey(1)); len(kvblob) == 0 {
					printChainMetadata(freezerDb)
					return nil, errors.New("ancient chain segments already extracted, please set --datadir.ancient to the correct path")
				}
				// Block #1 is still in the database, we're allowed to init a new freezer
			}
			// Otherwise, the head header is still the genesis, we're allowed to init a new
			// freezer.
		}
	}

	// Freezer is consistent with the key-value database, permit combining the two
	if !disableFreeze && !readonly {
		frdb.wg.Add(1)
		go func() {
			frdb.freeze(db, false)
			frdb.wg.Done()
		}()
	}
	return freezerDb, nil
}

// NewMemoryDatabase creates an ephemeral in-memory key-value database without a
// freezer moving immutable chain segments into cold storage.
func NewMemoryDatabase() ethdb.Database {
	return NewDatabase(memorydb.New())
}

const (
	DBPebble  = "pebble"
	DBLeveldb = "leveldb"
)

// PreexistingDatabase checks the given data directory whether a database is already
// instantiated at that location, and if so, returns the type of database (or the
// empty string).
func PreexistingDatabase(path string) string {
	if _, err := os.Stat(filepath.Join(path, "CURRENT")); err != nil {
		return "" // No pre-existing db
	}
	if matches, err := filepath.Glob(filepath.Join(path, "OPTIONS*")); len(matches) > 0 || err != nil {
		if err != nil {
			panic(err) // only possible if the pattern is malformed
		}
		return DBPebble
	}
	return DBLeveldb
}

type counter uint64

func (c counter) String() string {
	return fmt.Sprintf("%d", c)
}

func (c counter) Percentage(current uint64) string {
	return fmt.Sprintf("%d", current*100/uint64(c))
}

// stat stores sizes and count for a parameter
type stat struct {
	size  common.StorageSize
	count counter
}

// Add size to the stat and increase the counter by 1
func (s *stat) Add(size common.StorageSize) {
	s.size += size
	s.count++
}

func (s *stat) Size() string {
	return s.size.String()
}

func (s *stat) Count() string {
	return s.count.String()
}

func AncientInspect(db ethdb.Database) error {
	ancientTail, err := db.Tail()
	if err != nil {
		return err
	}
	ancientHead, err := db.Ancients()
	if err != nil {
		return err
	}
	stats := [][]string{
		{"Offset/StartBlockNumber", "Offset/StartBlockNumber of ancientDB", counter(ancientTail).String()},
		{"Amount of remained items in AncientStore", "Remaining items of ancientDB", counter(ancientHead - ancientTail).String()},
		{"The last BlockNumber within ancientDB", "The last BlockNumber", counter(ancientHead - 1).String()},
	}
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Database", "Category", "Items"})
	table.SetFooter([]string{"", "AncientStore information", ""})
	table.AppendBulk(stats)
	table.Render()

	return nil
}

func PruneHashTrieNodeInDataBase(db ethdb.Database) error {
	it := db.NewIterator([]byte{}, []byte{})
	defer it.Release()

	total_num := 0
	for it.Next() {
		var key = it.Key()
		switch {
		case IsLegacyTrieNode(key, it.Value()):
			db.Delete(key)
			total_num++
			if total_num%100000 == 0 {
				log.Info("Pruning hash-base state trie nodes", "Complete progress: ", total_num)
			}
		default:
			continue
		}
	}
	log.Info("Pruning hash-base state trie nodes", "Complete progress", total_num)
	return nil
}

type DataType int

const (
	StateDataType DataType = iota
	ChainDataType
	Unknown
)

func DataTypeByKey(key []byte) DataType {
	switch {
	// state
	case IsLegacyTrieNode(key, key),
		bytes.HasPrefix(key, stateIDPrefix) && len(key) == len(stateIDPrefix)+common.HashLength,
		IsAccountTrieNode(key),
		IsStorageTrieNode(key):
		return StateDataType

	default:
		for _, meta := range [][]byte{
			fastTrieProgressKey, persistentStateIDKey, trieJournalKey, snapSyncStatusFlagKey} {
			if bytes.Equal(key, meta) {
				return StateDataType
			}
		}
		return ChainDataType
	}
}

// KeyValuePair represents a key-value pair for batch writing
type KeyValuePair struct {
	Key   []byte
	Value []byte
	Type  string // "account" or "storage"
}

// BatchWriter handles batch writing to pebble db
type BatchWriter struct {
	db           *pebble.DB
	batch        *pebble.Batch
	batchSize    int
	maxBatchSize int
	mu           sync.Mutex
	writtenCount *int64
}

func NewBatchWriter(db *pebble.DB, maxBatchSize int, writtenCount *int64) *BatchWriter {
	return &BatchWriter{
		db:           db,
		batch:        db.NewBatch(),
		maxBatchSize: maxBatchSize,
		writtenCount: writtenCount,
	}
}

func (bw *BatchWriter) Add(kv KeyValuePair) error {
	bw.mu.Lock()
	defer bw.mu.Unlock()

	if err := bw.batch.Set(kv.Key, kv.Value, nil); err != nil {
		return err
	}

	bw.batchSize += len(kv.Key) + len(kv.Value)

	// Flush if batch size exceeds limit
	if bw.batchSize >= bw.maxBatchSize {
		return bw.flush()
	}

	return nil
}

func (bw *BatchWriter) flush() error {
	if bw.batch.Count() > 0 {
		if err := bw.batch.Commit(pebble.Sync); err != nil {
			return err
		}
		atomic.AddInt64(bw.writtenCount, int64(bw.batch.Count()))
		bw.batch.Close()
		bw.batch = bw.db.NewBatch()
		bw.batchSize = 0
	}
	return nil
}

func (bw *BatchWriter) Close() error {
	bw.mu.Lock()
	defer bw.mu.Unlock()

	// Flush remaining batch
	if err := bw.flush(); err != nil {
		return err
	}

	if bw.batch != nil {
		bw.batch.Close()
	}
	return nil
}

// InspectDatabase traverses the entire database and checks the size
// of all different categories of data.
// Modified to concurrently scan trie node prefixed data and batch write 10% to test-case pebble db using a thread pool.
func InspectDatabase(db ethdb.Database, keyPrefix, keyStart []byte) error {
	// Create or open test-case pebble db
	testCaseDir := "test-case"
	if err := os.MkdirAll(testCaseDir, 0755); err != nil {
		return fmt.Errorf("failed to create test-case directory: %v", err)
	}

	opts := &pebble.Options{}
	testDB, err := pebble.Open(testCaseDir, opts)
	if err != nil {
		return fmt.Errorf("failed to open test-case pebble db: %v", err)
	}
	defer testDB.Close()

	// Initialize random seed
	rand.Seed(time.Now().UnixNano())

	// Concurrent scan and batch write setup
	const maxBatchSize = 128 * 1024 * 1024 // 128MB
	const workerCount = 4                  // Thread pool size
	const channelBufferSize = 1000

	var (
		count        int64
		writtenCount int64
		start        = time.Now()
		logged       = time.Now()
		accountTries stat
		storageTries stat
		total        common.StorageSize
		wg           sync.WaitGroup
		scanWg       sync.WaitGroup
		kvChannel    = make(chan KeyValuePair, channelBufferSize)
		statMu       sync.Mutex
	)

	// Create batch writers (thread pool)
	writers := make([]*BatchWriter, workerCount)
	for i := 0; i < workerCount; i++ {
		writers[i] = NewBatchWriter(testDB, maxBatchSize, &writtenCount)
	}

	// Start worker goroutines (thread pool)
	wg.Add(workerCount)
	for i := 0; i < workerCount; i++ {
		go func(writerIdx int) {
			defer wg.Done()
			writer := writers[writerIdx]
			defer writer.Close()

			for kv := range kvChannel {
				if err := writer.Add(kv); err != nil {
					log.Error("Failed to write batch to test-case db",
						"worker", writerIdx,
						"key", fmt.Sprintf("%x", kv.Key[:min(8, len(kv.Key))]),
						"error", err)
				}
			}
		}(i)
	}

	// Concurrent scanning function
	scanIterator := func(prefix []byte, iterType string, statPtr *stat) {
		defer scanWg.Done()

		iterator := db.NewIterator(prefix, nil)
		defer iterator.Release()

		localCount := int64(0)
		localTotal := common.StorageSize(0)
		localStat := stat{}

		log.Info("Starting concurrent scan", "type", iterType)

		for iterator.Next() {
			key := make([]byte, len(iterator.Key()))
			value := make([]byte, len(iterator.Value()))
			copy(key, iterator.Key())
			copy(value, iterator.Value())

			size := common.StorageSize(len(key) + len(value))
			localTotal += size
			localStat.Add(size)
			localCount++

			// 10% probability to write to test-case db
			if rand.Intn(100) < 10 {
				kv := KeyValuePair{
					Key:   key,
					Value: value,
					Type:  iterType,
				}

				select {
				case kvChannel <- kv:
					log.Debug("Queued trie node for batch write",
						"type", iterType,
						"key", fmt.Sprintf("%x", key[:min(8, len(key))]))
				default:
					log.Warn("Channel buffer full, dropping key-value pair", "type", iterType)
				}
			}

			if localCount%1000 == 0 && time.Since(logged) > 8*time.Second {
				log.Info("Concurrent scanning progress",
					"type", iterType,
					"count", localCount,
					"elapsed", common.PrettyDuration(time.Since(start)))
				logged = time.Now()
			}
		}

		// Update global counters atomically
		atomic.AddInt64(&count, localCount)

		// Update total size and stat with mutex
		statMu.Lock()
		total += localTotal
		*statPtr = localStat
		statMu.Unlock()

		log.Info("Completed concurrent scan",
			"type", iterType,
			"scanned", localCount,
			"elapsed", common.PrettyDuration(time.Since(start)))
	}

	// Start concurrent scanning
	scanWg.Add(2)
	go scanIterator(TrieNodeAccountPrefix, "account", &accountTries)
	go scanIterator(TrieNodeStoragePrefix, "storage", &storageTries)

	// Wait for all scanning to complete
	scanWg.Wait()
	close(kvChannel) // Signal workers to finish

	// Wait for all batch writers to finish
	wg.Wait()

	// Display the trie node scan results
	log.Info("Concurrent trie node scan completed",
		"totalScanned", count,
		"writtenToTestCase", writtenCount,
		"writePercentage", fmt.Sprintf("%.2f%%", float64(writtenCount)*100/float64(count)),
		"elapsed", common.PrettyDuration(time.Since(start)))

	stats := [][]string{
		{"Trie Nodes", "Account trie nodes", accountTries.Size(), accountTries.Count()},
		{"Trie Nodes", "Storage trie nodes", storageTries.Size(), storageTries.Count()},
		{"Test Case", "Written to test-case db", "-", fmt.Sprintf("%d", writtenCount)},
	}

	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Database", "Category", "Size", "Items"})
	table.SetFooter([]string{"", "Total", total.String(), fmt.Sprintf("%d", count)})
	table.AppendBulk(stats)
	table.Render()

	return nil
}

func DeleteTrieState(db ethdb.Database) error {
	var (
		it     ethdb.Iterator
		batch  = db.NewBatch()
		start  = time.Now()
		logged = time.Now()
		count  int64
		key    []byte
	)

	prefixKeys := map[string]func([]byte) bool{
		string(TrieNodeAccountPrefix): IsAccountTrieNode,
		string(TrieNodeStoragePrefix): IsStorageTrieNode,
		string(stateIDPrefix):         func(key []byte) bool { return len(key) == len(stateIDPrefix)+common.HashLength },
	}

	for prefix, isValid := range prefixKeys {
		it = db.NewIterator([]byte(prefix), nil)

		for it.Next() {
			key = it.Key()
			if !isValid(key) {
				continue
			}

			batch.Delete(it.Key())
			if batch.ValueSize() > ethdb.IdealBatchSize {
				if err := batch.Write(); err != nil {
					it.Release()
					return err
				}
				batch.Reset()
			}

			count++
			if time.Since(logged) > 8*time.Second {
				log.Info("Deleting trie state", "count", count, "elapsed", common.PrettyDuration(time.Since(start)))
				logged = time.Now()
			}
		}

		it.Release()
	}

	if batch.ValueSize() > 0 {
		if err := batch.Write(); err != nil {
			return err
		}
		batch.Reset()
	}

	log.Info("Deleted trie state", "count", count, "elapsed", common.PrettyDuration(time.Since(start)))

	return nil
}

// printChainMetadata prints out chain metadata to stderr.
func printChainMetadata(db ethdb.Reader) {
	fmt.Fprintf(os.Stderr, "Chain metadata\n")
	for _, v := range ReadChainMetadata(db) {
		fmt.Fprintf(os.Stderr, "  %s\n", strings.Join(v, ": "))
	}
	fmt.Fprintf(os.Stderr, "\n\n")
}

// ReadChainMetadata returns a set of key/value pairs that contains information
// about the database chain status. This can be used for diagnostic purposes
// when investigating the state of the node.
func ReadChainMetadata(db ethdb.Reader) [][]string {
	pp := func(val *uint64) string {
		if val == nil {
			return "<nil>"
		}
		return fmt.Sprintf("%d (%#x)", *val, *val)
	}
	data := [][]string{
		{"databaseVersion", pp(ReadDatabaseVersion(db))},
		{"headBlockHash", fmt.Sprintf("%v", ReadHeadBlockHash(db))},
		{"headFastBlockHash", fmt.Sprintf("%v", ReadHeadFastBlockHash(db))},
		{"headHeaderHash", fmt.Sprintf("%v", ReadHeadHeaderHash(db))},
		{"lastPivotNumber", pp(ReadLastPivotNumber(db))},
		{"len(snapshotSyncStatus)", fmt.Sprintf("%d bytes", len(ReadSnapshotSyncStatus(db)))},
		{"snapshotDisabled", fmt.Sprintf("%v", ReadSnapshotDisabled(db))},
		{"snapshotJournal", fmt.Sprintf("%d bytes", len(ReadSnapshotJournal(db)))},
		{"snapshotRecoveryNumber", pp(ReadSnapshotRecoveryNumber(db))},
		{"snapshotRoot", fmt.Sprintf("%v", ReadSnapshotRoot(db))},
		{"txIndexTail", pp(ReadTxIndexTail(db))},
	}
	return data
}
