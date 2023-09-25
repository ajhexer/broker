package fsm

import (
	"broker/internal/raft/db"
	"encoding/json"
	"github.com/dgraph-io/badger/v4"
	"github.com/hashicorp/raft"
	"io"
	"log"
	"strconv"
	"sync"
)

type BrokerFSM struct {
	db          *db.BadgerDB
	lastIndexes map[string]int32
	lock        sync.Mutex
}

func NewFSM(path string) (*BrokerFSM, error) {
	var b db.BadgerDB
	err := b.Init(path)
	if err != nil {
		log.Fatal("FSM: Unable to Setup Database")
		return nil, err
	}

	log.Println("FSM: Initialised FSM")

	return &BrokerFSM{
		db:          &b,
		lastIndexes: make(map[string]int32),
		lock:        sync.Mutex{},
	}, nil
}

func (f *BrokerFSM) Apply(l *raft.Log) interface{} {
	var err error = nil
	var logData LogData
	err = json.Unmarshal(l.Data, &logData)
	if err != nil {
		return err
	}
	switch logData.Operation {
	case IncIndex:
		f.lock.Lock()
		if lastIndex, ok := f.lastIndexes[logData.Subject]; !ok || lastIndex < logData.NewIndex {
			f.lastIndexes[logData.Subject] = logData.NewIndex

			err = f.db.Write([]byte(logData.Subject), []byte(strconv.Itoa(int(logData.NewIndex))))
		}
		f.lock.Unlock()
	}

	return err
}

func (f *BrokerFSM) Snapshot() (raft.FSMSnapshot, error) {
	log.Println("BrokerFSM: BrokerSnapshot")
	return &BrokerSnapshot{
		db: f.db,
	}, nil
}

func (f *BrokerFSM) Restore(r io.ReadCloser) error {
	log.Println("BrokerFSM: Restore")
	err := f.db.Conn.DropAll()
	if err != nil {
		log.Fatal("BrokerFSM: Unable to delete previous state")
		return err
	}
	err = f.db.Conn.Load(r, 100)
	if err != nil {
		return err
	}
	if err != nil {
		return nil
	}
	return f.db.Conn.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			var value []byte
			if err := item.Value(func(val []byte) error {
				value = append([]byte{}, val...)
				return nil
			}); err != nil {
				return err
			}
			subject := string(item.Key())
			index, err := strconv.Atoi(string(value))
			if err != nil {
				return err
			}
			f.lastIndexes[subject] = int32(index)
		}
		return nil
	})
}

func (f *BrokerFSM) IncIndex(subject string) int32 {
	var index int32
	f.lock.Lock()
	if _, ok := f.lastIndexes[subject]; !ok {
		f.lastIndexes[subject] = 0
	}
	index = f.lastIndexes[subject]
	f.lastIndexes[subject]++
	f.lock.Unlock()
	return index
}
