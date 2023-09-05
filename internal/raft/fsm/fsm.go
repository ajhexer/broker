package fsm

import (
	"broker/internal/raft/db"
	"broker/internal/repository"
	"broker/pkg/broker"
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
	repo        repository.SecondaryDB
	lastIndexes map[string]int
	lock        sync.Mutex
}

func NewFSM(path string, repo repository.SecondaryDB) (*BrokerFSM, error) {
	var b db.BadgerDB
	err := b.Init(path)
	if err != nil {
		log.Fatal("FSM: Unable to Setup Database")
		return nil, err
	}

	log.Println("FSM: Initialised FSM")

	return &BrokerFSM{
		db:          &b,
		repo:        repo,
		lastIndexes: make(map[string]int),
		lock:        sync.Mutex{},
	}, nil
}

func (f *BrokerFSM) Apply(l *raft.Log) interface{} {
	var logData LogData
	err := json.Unmarshal(l.Data, &logData)
	if err != nil {
		return err
	}
	switch logData.Operation {
	case SAVE:
		return f.Save(logData)
	case DELETE:
		return f.Delete(logData.Subject, logData.Message.Id)
	}
	return nil
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
	err = f.repo.DropAll()
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
			var logData LogData
			err := json.Unmarshal(value, &logData)
			if err != nil {
				return err
			}
			if f.lastIndexes[logData.Subject] < logData.Message.Id {
				f.lastIndexes[logData.Subject] = logData.Message.Id + 1
			}
			f.repo.Save(logData.Message, logData.Subject)
		}
		return nil
	})
}

func (f *BrokerFSM) Save(logData LogData) error {
	msg := logData.Message
	subject := logData.Subject
	id := strconv.Itoa(msg.Id)
	msgBytes, err := json.Marshal(logData)
	if err != nil {
		return err
	}

	dbErrChan := make(chan error)
	repoErrChan := make(chan error)

	go func() {
		err := f.db.Write([]byte(id+subject), msgBytes)
		dbErrChan <- err
	}()

	go func() {
		err := f.repo.Save(msg, subject)
		repoErrChan <- err
	}()
	dbErr := <-dbErrChan
	repoErr := <-repoErrChan

	if dbErr != nil {
		return dbErr
	}

	if repoErr != nil {
		log.Fatal(repoErr)
	}

	return nil
}
func (f *BrokerFSM) Delete(subject string, index int) error {

	id := strconv.Itoa(index)
	err := f.db.Delete([]byte(id + subject))
	if err != nil {
		return err
	}
	return f.repo.DeleteMessage(index, subject)
}

func (f *BrokerFSM) Get(subject string, index int) (broker.Message, error) {
	return f.repo.FetchMessage(index, subject)

}
func (f *BrokerFSM) IncIndex(subject string) int {
	var index int
	f.lock.Lock()
	if _, ok := f.lastIndexes[subject]; !ok {
		f.lastIndexes[subject] = 0
	}
	index = f.lastIndexes[subject]
	f.lastIndexes[subject]++
	f.lock.Unlock()
	return index
}
