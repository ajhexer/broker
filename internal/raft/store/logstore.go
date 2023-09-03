package store

import (
	"github.com/dgraph-io/badger/v4"
	"log"

	"broker/internal/raft/db"

	"github.com/hashicorp/raft"
)

type RavelLogStore struct {
	Db *db.BadgerDB
}

func NewRavelLogStore(logDBPath string) (*RavelLogStore, error) {
	var ravelDB db.BadgerDB
	err := ravelDB.Init(logDBPath)
	if err != nil {
		log.Fatalf("NewRavelLogStore: %v\n", err)
		return nil, err
	}

	log.Println("LogStore: Initialised Log Store")
	return &RavelLogStore{
		Db: &ravelDB,
	}, nil
}

func (r *RavelLogStore) FirstIndex() (uint64, error) {
	var key uint64
	err := r.Db.Conn.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()

		it.Rewind()
		if it.Valid() {
			firstKey := it.Item().Key()
			key = bytesToUint64(firstKey)
		} else {
			key = 0
		}

		return nil
	})

	if err != nil {
		log.Fatalf("RavelLogStore.FirstIndex: %v\n", err)
		return 0, err
	}

	return key, nil
}

func (r *RavelLogStore) LastIndex() (uint64, error) {
	var key uint64
	err := r.Db.Conn.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Reverse = true
		it := txn.NewIterator(opts)
		defer it.Close()

		it.Rewind()
		if it.Valid() {
			firstKey := it.Item().Key()
			key = bytesToUint64(firstKey)
		} else {
			key = 0
		}

		return nil
	})

	if err != nil {
		log.Fatalf("RavelLogStore.LastIndex: %v\n", err)
		return 0, err
	}
	return key, nil
}

func (r *RavelLogStore) GetLog(index uint64, raftLog *raft.Log) error {
	key := uint64ToBytes(index)
	val, err := r.Db.Read(key)
	if err != nil {
		log.Printf("RavelLogStore.GetLog: %v\n", err)
		return err
	}

	return bytesToRaftLog(val, raftLog)
}

func (r *RavelLogStore) StoreLog(l *raft.Log) error {
	return r.StoreLogs([]*raft.Log{l})
}

func (r *RavelLogStore) StoreLogs(logs []*raft.Log) error {
	for _, l := range logs {
		key := uint64ToBytes(l.Index)
		val := raftLogToBytes(*l)

		err := r.Db.Write(key, val)
		if err != nil {
			log.Fatalf("RavelLogStore.StoreLogs: %v\n", err)
			return err
		}
	}

	return nil
}

func (r *RavelLogStore) DeleteRange(min uint64, max uint64) error {
	minKey := uint64ToBytes(min)

	txn := r.Db.Conn.NewTransaction(true)
	defer txn.Discard()

	opts := badger.DefaultIteratorOptions
	it := txn.NewIterator(opts)
	it.Seek(minKey)

	for {
		key := it.Item().Key()

		if bytesToUint64(key) > max {
			break
		}

		err := r.Db.Delete(key)
		if err != nil {
			return err
		}

		it.Next()
	}

	if err := txn.Commit(); err != nil {
		log.Fatalf("RavelLogStore.DeleteRange: %v\n", err)
		return err
	}
	return nil
}
