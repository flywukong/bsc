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
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/ethdb/memorydb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/olekukonko/tablewriter"
)

// freezerdb is a database wrapper that enables ancient chain segment freezing.
type freezerdb struct {
	ethdb.KeyValueStore
	ethdb.AncientStore

	readOnly    bool
	ancientRoot string

	ethdb.AncientFreezer
	stateStore ethdb.Database
}

func (frdb *freezerdb) StateStoreReader() ethdb.Reader {
	if frdb.stateStore == nil {
		return frdb
	}
	return frdb.stateStore
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
	stateStore ethdb.Database
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

func (db *emptyfreezedb) GetStateStore() ethdb.Database      { return db }
func (db *emptyfreezedb) SetStateStore(state ethdb.Database) {}
func (db *emptyfreezedb) StateStoreReader() ethdb.Reader     { return db }
func (db *emptyfreezedb) HasSeparateStateStore() bool        { return false }
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

// InspectDatabase traverses the entire database and checks the size
// of all different categories of data.
func InspectDatabase(db ethdb.Database, keyPrefix, keyStart []byte) error {
	return InspectDatabaseWithExpansion(db, nil, keyPrefix, keyStart, 1)
}

// InspectDatabaseWithExpansion traverses the source database and optionally
// expands data to a target database (1T -> 2T expansion)
func InspectDatabaseWithExpansion(sourceDb ethdb.Database, targetDb ethdb.Database, keyPrefix, keyStart []byte, suffix byte) error {
	// If no target database provided, run normal inspection
	if targetDb == nil {
		return inspectDatabaseNormal(sourceDb, keyPrefix, keyStart)
	}

	// Run database expansion from source to target
	return expandDatabase(sourceDb, targetDb, keyPrefix, keyStart, suffix)
}

// inspectDatabaseNormal performs normal database inspection without expansion
func inspectDatabaseNormal(db ethdb.Database, keyPrefix, keyStart []byte) error {
	// TODO: Implement normal inspection logic if needed
	log.Info("Normal database inspection not implemented, use geth db inspect command instead")
	return nil
}

// expandDatabase performs 1T -> 2T database expansion from source to target
// Now uses focused concurrent prefix-based scanning for better performance
func expandDatabase(sourceDb, targetDb ethdb.Database, keyPrefix, keyStart []byte, suffix byte) error {
	log.Info("Starting database expansion with focused concurrent scanning",
		"sourceDb", "read-only", "targetDb", "write-only", "suffix", suffix,
		"mode", "focused_concurrent_prefix_based",
		"targetDataTypes", "accountTrie,storageTrie,code,txLookup,accountSnapshot,storageSnapshot,headers,blockBodies,blockReceipts,headerTDs,blobSidecars,headerHashes,headerNumbers,stateIDs,bloomBits")

	// Use the new focused expansion implementation for better performance
	return expandDatabaseFocused(sourceDb, targetDb, suffix)
}

// kvPair represents a key-value pair for the focused expansion
type kvPair struct {
	key   []byte
	value []byte
	size  int
}

// expandDatabaseFocused performs concurrent database expansion focusing on specific data types
// This version uses prefix-based concurrent scanning for better performance
func expandDatabaseFocused(sourceDb ethdb.Database, targetDb ethdb.Database, suffix byte) error {
	const (
		totalExpectedKeys = 18800000000       // 18.8 billion keys
		numWriters        = 45                // Number of writer goroutines
		maxBatchSize      = 512 * 1024 * 1024 // 512MB batch size
		channelBufferSize = 10000             // Channel buffer size
	)

	// Trie node skipping counters
	var (
		skippedAccountTries int64 // 跳过的account trie节点计数
		skippedStorageTries int64 // 跳过的storage trie节点计数
	)

	log.Info("run with new version")
	// Define focused prefixes to scan concurrently - only the data types we want
	prefixes := []struct {
		prefix  []byte
		name    string
		checker func([]byte) bool
	}{
		{TrieNodeAccountPrefix, "accountTrie", IsAccountTrieNode},
		{TrieNodeStoragePrefix, "storageTrie", IsStorageTrieNode},
		{CodePrefix, "code", func(key []byte) bool {
			return bytes.HasPrefix(key, CodePrefix) && len(key) == len(CodePrefix)+common.HashLength
		}},
		{txLookupPrefix, "txLookup", func(key []byte) bool {
			return bytes.HasPrefix(key, txLookupPrefix) && len(key) == (len(txLookupPrefix)+common.HashLength)
		}},
		{SnapshotAccountPrefix, "accountSnapshot", func(key []byte) bool {
			return bytes.HasPrefix(key, SnapshotAccountPrefix) && len(key) == (len(SnapshotAccountPrefix)+common.HashLength)
		}},
		{SnapshotStoragePrefix, "storageSnapshot", func(key []byte) bool {
			return bytes.HasPrefix(key, SnapshotStoragePrefix) && len(key) == (len(SnapshotStoragePrefix)+2*common.HashLength)
		}},
		{headerPrefix, "headers", func(key []byte) bool {
			return bytes.HasPrefix(key, headerPrefix) && len(key) == (len(headerPrefix)+8+common.HashLength)
		}},
		{blockBodyPrefix, "blockBodies", func(key []byte) bool {
			return bytes.HasPrefix(key, blockBodyPrefix) && len(key) == (len(blockBodyPrefix)+8+common.HashLength)
		}},
		{blockReceiptsPrefix, "blockReceipts", func(key []byte) bool {
			return bytes.HasPrefix(key, blockReceiptsPrefix) && len(key) == (len(blockReceiptsPrefix)+8+common.HashLength)
		}},
		{headerPrefix, "headerTDs", func(key []byte) bool {
			return bytes.HasPrefix(key, headerPrefix) && bytes.HasSuffix(key, headerTDSuffix)
		}},
		{BlockBlobSidecarsPrefix, "blobSidecars", func(key []byte) bool {
			return bytes.HasPrefix(key, BlockBlobSidecarsPrefix)
		}},
		{headerPrefix, "headerHashes", func(key []byte) bool {
			return bytes.HasPrefix(key, headerPrefix) && bytes.HasSuffix(key, headerHashSuffix)
		}},
		{headerNumberPrefix, "headerNumbers", func(key []byte) bool {
			return bytes.HasPrefix(key, headerNumberPrefix) && len(key) == (len(headerNumberPrefix)+common.HashLength)
		}},
		{stateIDPrefix, "stateIDs", func(key []byte) bool {
			return bytes.HasPrefix(key, stateIDPrefix) && len(key) == len(stateIDPrefix)+common.HashLength
		}},
		{bloomBitsPrefix, "bloomBits", func(key []byte) bool {
			return bytes.HasPrefix(key, bloomBitsPrefix) && len(key) == (len(bloomBitsPrefix)+10+common.HashLength)
		}},
	}

	// Shared statistics
	var (
		newKeysCreated    int64
		totalBytesWritten int64
		writeErrors       int64
		duplicateKeys     int64
		duplicateValues   int64
		skippedKeys       int64
	)

	// Shared channel for key-value pairs
	kvChan := make(chan kvPair, channelBufferSize)

	log.Info("Starting focused concurrent database expansion from 1T to 2T",
		"writers", numWriters, "maxBatchSize", "512MB", "expectedKeys", totalExpectedKeys,
		"focusedPrefixes", len(prefixes), "sourceDb", "read-only", "targetDb", "write-only",
		"version", suffix, "scanStrategy", "focused_concurrent_prefix_based",
		"targetDataTypes", "accountTrie,storageTrie,code,txLookup,accountSnapshot,storageSnapshot,headers,blockBodies,blockReceipts,headerTDs,blobSidecars,headerHashes,headerNumbers,stateIDs,bloomBits")

	// Helper functions (same transformation logic as before)
	shuffleValue := func(originalValue []byte, version byte) []byte {
		// Always create new value with same length as original
		newValue := make([]byte, len(originalValue))
		copy(newValue, originalValue)

		// Apply deterministic shuffle pattern
		for i := 0; i < len(newValue)/2; i++ {
			j := (i + len(newValue)/2) % len(newValue)
			newValue[i], newValue[j] = newValue[j], newValue[i]
		}

		// Apply random XOR transformation
		var xorKey [1]byte
		rand.Read(xorKey[:])
		for i := range newValue {
			newValue[i] ^= xorKey[0]
		}

		// Special handling for 1-byte values: ensure they are different from original
		if len(newValue) == 1 && bytes.Equal(newValue, originalValue) {
			// Simply increment by 1 to ensure difference
			newValue[0] = originalValue[0] + 1
		}

		// Special handling for 2-5 byte values: if still same, shuffle again
		if len(newValue) > 1 && len(newValue) <= 5 && bytes.Equal(newValue, originalValue) {
			// Apply another round of shuffle
			for i := 0; i < len(newValue)/2; i++ {
				j := (i + len(newValue)/2) % len(newValue)
				newValue[i], newValue[j] = newValue[j], newValue[i]
			}

			// Apply another random XOR transformation
			var xorKey2 [1]byte
			rand.Read(xorKey2[:])
			for i := range newValue {
				newValue[i] ^= xorKey2[0]
			}
		}

		return newValue
	}

	generateNewKey := func(originalKey []byte, suffix byte) []byte {
		if len(originalKey) <= 2 {
			return nil
		}

		// Create new key with length + 1 to append suffix
		newKey := make([]byte, len(originalKey)+1)
		copy(newKey, originalKey)

		// 保持第一个字节不变（前缀一致）
		// newKey[0] 已经通过 copy(newKey, originalKey) 设置为 originalKey[0]

		// 填充中间部分随机数据 (excluding first and last byte of original key)
		if len(originalKey) > 2 {
			randomBytes := make([]byte, len(originalKey)-2)
			rand.Read(randomBytes)
			copy(newKey[1:len(originalKey)-1], randomBytes)
		}

		// 设置倒数第二个字节为 's'，最后一个字节为 suffix
		newKey[len(newKey)-2] = 's'    // 倒数第二个字节 (原来hash的最后一个字节)
		newKey[len(newKey)-1] = suffix // 最后一个字节 (新增的)

		// 检查生成的key是否与原始key相同，如果相同则跳过
		if bytes.Equal(newKey[:len(originalKey)], originalKey) && len(newKey) == len(originalKey) {
			return nil // 返回nil表示跳过这个key
		}

		return newKey
	}

	// Additional variables needed for the worker goroutines
	var (
		processedCount int64
		lastProgress   int
		wg             sync.WaitGroup
	)

	// Start writer goroutines
	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			batch := targetDb.NewBatch() // Write to target database
			currentBatchSize := 0
			batchStartTime := time.Now()

			for kv := range kvChan {
				newKey := generateNewKey(kv.key, suffix)

				// Skip if generateNewKey returned nil (indicating key should be skipped)
				if newKey == nil {
					atomic.AddInt64(&skippedKeys, 1)
					keyLen := len(kv.key)
					if keyLen > 16 {
						keyLen = 16
					}
					log.Debug("Key skipped due to same key generation",
						"originalKey", fmt.Sprintf("%x", kv.key[:keyLen]))
					continue
				}

				// Check if new key is the same as original key (should not happen now)
				if bytes.Equal(newKey, kv.key) {
					atomic.AddInt64(&duplicateKeys, 1)
					keyLen := len(kv.key)
					if keyLen > 16 {
						keyLen = 16
					}
					log.Error("CRITICAL: New key is same as original key",
						"originalKey", fmt.Sprintf("%x", kv.key[:keyLen]),
						"originalKeyLen", len(kv.key),
						"newKey", fmt.Sprintf("%x", newKey[:keyLen]),
						"newKeyLen", len(newKey))
					continue
				}
				newValue := shuffleValue(kv.value, suffix)

				// Check if new value is the same as original value (should never happen with version byte)
				if bytes.Equal(newValue, kv.value) {
					atomic.AddInt64(&duplicateValues, 1)
					valueLen := len(kv.value)
					if valueLen > 16 {
						valueLen = 16
					}
					log.Error("CRITICAL: New value is same as original value",
						"originalValue", fmt.Sprintf("%x", kv.value[:valueLen]),
						"originalValueLen", len(kv.value),
						"newValue", fmt.Sprintf("%x", newValue[:valueLen]),
						"newValueLen", len(newValue))
					continue
				}

				if err := batch.Put(newKey, newValue); err != nil {
					atomic.AddInt64(&writeErrors, 1)
					log.Error("Failed to add to batch", "err", err, "worker", workerID)
					continue
				}

				currentBatchSize += len(newKey) + len(newValue)
				atomic.AddInt64(&newKeysCreated, 1)
				atomic.AddInt64(&totalBytesWritten, int64(len(newKey)+len(newValue)))

				// Check if batch should be written
				if currentBatchSize >= maxBatchSize {
					if err := batch.Write(); err != nil {
						atomic.AddInt64(&writeErrors, 1)
						log.Error("Batch write failed", "err", err, "worker", workerID, "size", currentBatchSize)
					} else {
						// Increment processedCount only after successful batch write
						atomic.AddInt64(&processedCount, int64(batch.ValueSize()/1024)) // Approximate key count in this batch

						writeDuration := time.Since(batchStartTime)
						throughput := float64(currentBatchSize) / writeDuration.Seconds() / (1024 * 1024) // MB/s
						log.Debug("Batch written",
							"worker", workerID,
							"size", common.StorageSize(currentBatchSize).String(),
							"duration", writeDuration,
							"throughput", fmt.Sprintf("%.2f MB/s", throughput))

						// Progress reporting every 10%
						processed := atomic.LoadInt64(&processedCount)
						progress := int(processed * 100 / totalExpectedKeys)
						if progress >= lastProgress+10 && progress != lastProgress {
							lastProgress = progress
							totalBytes := atomic.LoadInt64(&totalBytesWritten)
							currentSkippedAccountTries := atomic.LoadInt64(&skippedAccountTries)
							currentSkippedStorageTries := atomic.LoadInt64(&skippedStorageTries)
							log.Info("Data expansion progress",
								"progress", fmt.Sprintf("%d%%", progress),
								"processedKeys", processed,
								"totalBytesWritten", common.StorageSize(totalBytes).String(),
								"newKeysCreated", atomic.LoadInt64(&newKeysCreated),
								"skippedTrieNodes", fmt.Sprintf("账户:%d, 存储:%d, 总计:%d",
									currentSkippedAccountTries, currentSkippedStorageTries,
									currentSkippedAccountTries+currentSkippedStorageTries),
								"duplicateKeys", atomic.LoadInt64(&duplicateKeys))
						}
					}

					batch = targetDb.NewBatch() // Create new batch for target database
					currentBatchSize = 0
					batchStartTime = time.Now()
				}
			}

			// Write remaining batch
			if currentBatchSize > 0 {
				if err := batch.Write(); err != nil {
					atomic.AddInt64(&writeErrors, 1)
					log.Error("Final batch write failed", "err", err, "worker", workerID)
				} else {
					log.Info("Final batch written", "worker", workerID, "size", common.StorageSize(currentBatchSize).String())
				}
			}
		}(i)
	}

	// Start concurrent prefix scanners for focused data types
	var scannerWg sync.WaitGroup
	for _, prefixInfo := range prefixes {
		scannerWg.Add(1)
		go func(prefix []byte, name string, checker func([]byte) bool) {
			defer scannerWg.Done()

			log.Info("🔍 Starting focused prefix scanner",
				"prefix", name, "prefixHex", fmt.Sprintf("%x", prefix))

			// Create iterator for this prefix
			it := sourceDb.NewIterator(prefix, nil)
			defer it.Release()

			var prefixScanned int64
			for it.Next() {
				key := it.Key()
				value := it.Value()

				// Apply specific checker function
				if !checker(key) {
					atomic.AddInt64(&skippedKeys, 1)
					continue
				}

				// Send to writer channel
				kv := kvPair{
					key:   common.CopyBytes(key),
					value: common.CopyBytes(value),
					size:  len(key) + len(value),
				}
				kvChan <- kv
				prefixScanned++

				// Progress logging for this prefix
				if prefixScanned%500000 == 0 {
					log.Info("📊 Prefix scanning progress",
						"prefix", name, "scanned", prefixScanned)
				}
			}

			log.Info("✅ Prefix scanning completed",
				"prefix", name, "totalScanned", prefixScanned)
		}(prefixInfo.prefix, prefixInfo.name, prefixInfo.checker)
	}

	// Wait for all scanners to complete
	scannerWg.Wait()
	close(kvChan) // Signal writers to finish

	// Wait for all writers to complete
	wg.Wait()

	// Final statistics
	finalKeysCreated := atomic.LoadInt64(&newKeysCreated)
	finalBytesWritten := atomic.LoadInt64(&totalBytesWritten)
	finalErrors := atomic.LoadInt64(&writeErrors)
	finalDuplicates := atomic.LoadInt64(&duplicateKeys)
	finalDuplicateValues := atomic.LoadInt64(&duplicateValues)
	finalSkipped := atomic.LoadInt64(&skippedKeys)
	finalSkippedAccountTries := atomic.LoadInt64(&skippedAccountTries)
	finalSkippedStorageTries := atomic.LoadInt64(&skippedStorageTries)
	totalSkippedTries := finalSkippedAccountTries + finalSkippedStorageTries

	log.Info("🎉 Focused concurrent database expansion completed!",
		"newKeysCreated", finalKeysCreated,
		"totalBytesWritten", common.StorageSize(finalBytesWritten).String(),
		"writeErrors", finalErrors,
		"duplicateKeys", finalDuplicates,
		"duplicateValues", finalDuplicateValues,
		"skippedKeys", finalSkipped,
		"focusedDataTypes", "accountTrie,storageTrie,code,txLookup,accountSnapshot,storageSnapshot")

	log.Info("📊 Trie Node Skipping Statistics",
		"skippedAccountTrieNodes", finalSkippedAccountTries,
		"skippedStorageTrieNodes", finalSkippedStorageTries,
		"totalSkippedTrieNodes", totalSkippedTries,
		"reason", "避免trie节点冲突，只生成非trie数据的冗余版本")

	if finalDuplicates > 0 {
		log.Warn("Duplicate key generation detected", "duplicateCount", finalDuplicates)
	}

	return nil
}

// KeyValuePair represents a key-value pair for async processing
type KeyValuePair struct {
	Key   []byte
	Value []byte
}

// DeleteRedundantTxLookupData deletes redundant txlookup data that was generated during database expansion
// 使用11线程极速异步删除冗余txlookup数据（无备份，最大化性能）
// Architecture: 1 scan thread + 10 delete worker threads
// Redundant keys are identified by:
// 1. Having txlookup prefix ("l")
// 2. Length of 35 bytes (normal is 33 bytes)
// 3. Second-to-last byte is 's'
// 4. Last byte is suffix (1)
func DeleteRedundantTxLookupData(db ethdb.Database, _ string) error {
	const (
		normalTxLookupKeyLen    = 1 + 32  // prefix(1) + hash(32) = 33 bytes
		redundantTxLookupKeyLen = 35      // actual length based on logs: 35 bytes
		suffix                  = byte(1) // hardcoded suffix used in generateNewKey
		targetSizeGB            = 300.0   // Fixed 300GB deletion target
		logInterval             = 8 * time.Second
		batchSize               = 10000 // Process in smaller batches for better control
	)

	targetBytes := int64(targetSizeGB * 1024 * 1024 * 1024)

	// High-performance delete-only mode (no backup needed for maximum speed)

	// High-performance delete processing variables
	var (
		deletedCount      int64
		deletedBytes      int64
		matchedCount      int64 // 匹配到的冗余key计数
		totalCheckedKeys  int64
		totalTxLookupKeys int64                 // 总的txlookup key数量
		keyLengthStats    = make(map[int]int64) // 长度统计
		start             = time.Now()
		logged            = time.Now()
	)

	// Channels for high-performance delete processing
	const (
		channelBufferSize = 10000 // Increased buffer for 10 delete workers
		numDeleteWorkers  = 10    // 10 delete worker threads for maximum speed
	)
	deleteChannel := make(chan []byte, channelBufferSize) // For delete threads (only keys needed)
	doneChan := make(chan struct{})
	var wg sync.WaitGroup

	log.Info("开始11线程超高性能异步删除冗余txlookup数据",
		"targetDeleteSize", common.StorageSize(targetBytes),
		"redundantKeyLength", redundantTxLookupKeyLen,
		"normalKeyLength", normalTxLookupKeyLen,
		"suffix", suffix,
		"mode", "11线程极速删除",
		"architecture", "1扫描+10删除",
		"channelBuffer", channelBufferSize)

	// Start 10 high-performance delete worker threads for maximum speed
	for i := 0; i < numDeleteWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			deleteBatch := db.NewBatch()
			deleteCount := int64(0)
			deleteLogged := time.Now()

			for {
				select {
				case key := <-deleteChannel:
					// Delete the key
					deleteBatch.Delete(key)
					deleteCount++

					// Write delete batch when it gets large enough
					if deleteBatch.ValueSize() > ethdb.IdealBatchSize {
						if err := deleteBatch.Write(); err != nil {
							log.Error("删除批次写入失败", "worker", workerID, "err", err)
							return
						}
						deleteBatch.Reset()
					}

					// Process in controlled batches to avoid memory issues
					if deleteCount%batchSize == 0 {
						// Write delete batch
						if err := deleteBatch.Write(); err != nil {
							log.Error("删除批次写入失败", "worker", workerID, "err", err)
							return
						}
						deleteBatch.Reset()
						runtime.GC() // Force garbage collection periodically
					}

					// Log delete progress every 30 seconds (less frequent for 10 workers)
					if time.Since(deleteLogged) > 30*time.Second {
						log.Info("🗑️  删除工作线程进度",
							"workerID", workerID,
							"deleteCount", deleteCount,
							"totalDeletedSize", common.StorageSize(atomic.LoadInt64(&deletedBytes)),
							"batchSize", common.StorageSize(deleteBatch.ValueSize()),
							"channelBuffer", fmt.Sprintf("%d/%d", len(deleteChannel), cap(deleteChannel)),
							"elapsed", common.PrettyDuration(time.Since(start)))
						deleteLogged = time.Now()
					}

				case <-doneChan:
					// Write final delete batch
					if deleteBatch.ValueSize() > 0 {
						if err := deleteBatch.Write(); err != nil {
							log.Error("最终删除批次写入失败", "worker", workerID, "err", err)
						}
					}
					log.Info("✅ 删除工作线程完成", "workerID", workerID, "processedCount", deleteCount)
					return
				}
			}
		}(i)
	}

	// Create iterator for txlookup prefix
	it := db.NewIterator(txLookupPrefix, nil)
	defer it.Release()

	for it.Next() && deletedBytes < targetBytes {
		key := it.Key()
		totalCheckedKeys++

		// 统计所有txlookup key
		totalTxLookupKeys++
		keyLen := len(key)
		keyLengthStats[keyLen]++

		// 每1万个key打印一次长度统计
		if totalTxLookupKeys%10000 == 0 {
			log.Info("📊 txlookup数据扫描统计",
				"totalTxLookupKeys", totalTxLookupKeys,
				"lengthDistribution", keyLengthStats,
				"elapsed", common.PrettyDuration(time.Since(start)))
		}

		// 显示前几个key的详细信息用于调试
		if totalTxLookupKeys <= 5 {
			keyHex := fmt.Sprintf("%x", key)
			log.Info("🔍 txlookup样本key详情",
				"index", totalTxLookupKeys,
				"keyHex", keyHex,
				"keyLength", keyLen,
				"lastByte", key[keyLen-1],
				"secondLastByte", func() string {
					if keyLen >= 2 {
						return fmt.Sprintf("'%c'(%d)", key[keyLen-2], key[keyLen-2])
					}
					return "N/A"
				}())
		}

		// Check if this is a redundant txlookup key
		// Redundant keys have: length=34, last byte=suffix(1), second-to-last byte='s'
		if len(key) == redundantTxLookupKeyLen &&
			key[len(key)-1] == suffix &&
			key[len(key)-2] == 's' {
			// This appears to be a redundant key - send to delete workers
			matchedCount++
			value := it.Value()
			valueSize := len(value)

			// 每匹配1000个key打印一次进度
			if matchedCount%1000 == 0 {
				keyPrefix := fmt.Sprintf("%x", key[:8])
				log.Info("🔍 主扫描线程进度",
					"matchedCount", matchedCount,
					"currentKey", keyPrefix,
					"keyLength", len(key),
					"totalChecked", totalCheckedKeys,
					"deleteBuffer", fmt.Sprintf("%d/%d", len(deleteChannel), cap(deleteChannel)),
					"elapsed", common.PrettyDuration(time.Since(start)))
			}

			// Send to delete workers asynchronously
			select {
			case deleteChannel <- key:
				// Successfully sent to delete thread
			default:
				// Channel is full, wait a bit and retry
				time.Sleep(1 * time.Millisecond)
				deleteChannel <- key
			}

			// Update statistics
			deletedCount++
			atomic.AddInt64(&deletedBytes, int64(len(key)+valueSize))

			// Log progress periodically
			if time.Since(logged) > logInterval {
				currentDeletedBytes := atomic.LoadInt64(&deletedBytes)
				remainingBytes := targetBytes - currentDeletedBytes
				progress := float64(currentDeletedBytes) / float64(targetBytes) * 100
				log.Info("📡 主扫描线程总体进度",
					"scannedCount", deletedCount,
					"deletedSize", common.StorageSize(currentDeletedBytes),
					"matchedCount", matchedCount,
					"remainingSize", common.StorageSize(remainingBytes),
					"progress", fmt.Sprintf("%.1f%%", progress),
					"deleteBuffer", fmt.Sprintf("%d/%d", len(deleteChannel), cap(deleteChannel)),
					"totalChecked", totalCheckedKeys,
					"workers", numDeleteWorkers,
					"elapsed", common.PrettyDuration(time.Since(start)))
				logged = time.Now()
			}

			// Stop when we reach target size
			if atomic.LoadInt64(&deletedBytes) >= targetBytes {
				break
			}
		}
	}

	// Signal all delete worker threads to finish and wait
	close(deleteChannel) // Close delete channel
	close(doneChan)      // Signal threads to finish
	log.Info("等待10个删除工作线程完成...")
	wg.Wait()
	log.Info("📡 主扫描线程完成，10个删除工作线程已同步完成")

	// 显示最终统计信息
	log.Info("📊 最终txlookup数据统计",
		"totalTxLookupKeys", totalTxLookupKeys,
		"keyLengthDistribution", keyLengthStats)

	if matchedCount > 0 {
		finalDeletedBytes := atomic.LoadInt64(&deletedBytes)
		log.Info("✅ 11线程极速异步冗余txlookup数据删除完成",
			"deletedCount", deletedCount,
			"deletedSize", common.StorageSize(finalDeletedBytes),
			"matchedCount", matchedCount,
			"totalTxLookupKeys", totalTxLookupKeys,
			"matchRate", fmt.Sprintf("%.2f%%", float64(matchedCount)/float64(totalTxLookupKeys)*100),
			"mode", "11线程极速删除",
			"architecture", "1扫描+10删除",
			"workers", numDeleteWorkers,
			"elapsed", common.PrettyDuration(time.Since(start)),
			"avgKeySize", fmt.Sprintf("%.1f bytes", float64(finalDeletedBytes)/float64(deletedCount)))
	} else {
		// Still need to signal worker threads to finish even if no data found
		close(deleteChannel)
		close(doneChan)
		wg.Wait()

		log.Info("ℹ️  未找到符合条件的冗余txlookup数据",
			"totalTxLookupKeys", totalTxLookupKeys,
			"targetLength", redundantTxLookupKeyLen,
			"targetSuffix", suffix,
			"targetSecondLastByte", "'s'",
			"mode", "11线程极速删除",
			"architecture", "1扫描+10删除",
			"workers", numDeleteWorkers,
			"elapsed", common.PrettyDuration(time.Since(start)))
		log.Info("💡 可能的原因：1) 数据库中没有冗余数据 2) 生成的key格式与预期不匹配 3) 识别条件需要调整")
	}

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

// getDataTypeDescription returns a human-readable description of each data type
func getDataTypeDescription(name string) string {
	descriptions := map[string]string{
		"accountTrie":     "Account trie nodes (state data)",
		"storageTrie":     "Storage trie nodes (contract storage)",
		"code":            "Smart contract bytecode",
		"txLookup":        "Transaction hash to block lookup",
		"accountSnapshot": "Account state snapshots",
		"storageSnapshot": "Storage state snapshots",
	}
	if desc, exists := descriptions[name]; exists {
		return desc
	}
	return "Unknown data type"
}

// InspectDatabaseWithExpansionFocused uses focused concurrent prefix-based scanning for specific data types
// This version only processes: accountTrie, storageTrie, code, txLookup, accountSnapshot, storageSnapshot
func InspectDatabaseWithExpansionFocused(sourceDb ethdb.Database, targetDb ethdb.Database, suffix byte) error {
	// If no target database provided, return error
	if targetDb == nil {
		return fmt.Errorf("target database is required for expansion")
	}

	// Run focused concurrent database expansion from source to target
	return expandDatabaseFocused(sourceDb, targetDb, suffix)
}
