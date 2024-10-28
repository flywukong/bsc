package bboltdb

import (
	"testing"

	"github.com/etcd-io/bbolt"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/ethdb/dbtest"
)

func TestBoltDB(t *testing.T) {
	t.Run("DatabaseSuite", func(t *testing.T) {
		dbtest.TestDatabaseSuite(t, func() ethdb.KeyValueStore {
			options := &bbolt.Options{Timeout: 0}
			db1, err := bbolt.Open("bbolt.db", 0600, options)
			if err != nil {
				t.Fatalf("failed to open bbolt database: %v", err)
			}
			return &Database{
				db: db1,
			}
		})
	})
}
