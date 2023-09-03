package store

import (
	"broker/internal/raft/db"
	"github.com/dgraph-io/badger/v4"
	"log"
)

type RavelStableStore struct {
	Db *db.BadgerDB
}

func NewRavelStableStore(stableStoreDBPath string) (*RavelStableStore, error) {
	var ravelDB db.BadgerDB
	err := ravelDB.Init(stableStoreDBPath)
	if err != nil {
		log.Fatal("StableStore: Unable to setup new Stable Store")
		return nil, err
	}

	log.Println("StableStore: Initialised Stable Store")

	return &RavelStableStore{
		Db: &ravelDB,
	}, nil
}

func (s *RavelStableStore) Set(key []byte, val []byte) error {
	return s.Db.Write(key, val)
}

func (s *RavelStableStore) Get(key []byte) ([]byte, error) {
	val, err := s.Db.Read(key)
	if err != nil {
		if err.Error() == badger.ErrKeyNotFound.Error() {
			val = []byte{}
			return val, nil
		} else {
			log.Fatalln("StableStore: Error retrieving key from db")
		}
	}

	return val, nil
}

func (s *RavelStableStore) SetUint64(key []byte, val uint64) error {
	return s.Db.Write(key, uint64ToBytes(val))
}

func (s *RavelStableStore) GetUint64(key []byte) (uint64, error) {
	valBytes, err := s.Db.Read(key)

	var valUInt uint64
	if err != nil {
		if err.Error() == badger.ErrKeyNotFound.Error() {
			valUInt = 0
			return valUInt, nil
		} else {
			log.Fatalln("StableStore: Error retrieving key from db")
		}
	}

	valUInt = bytesToUint64(valBytes)
	return valUInt, nil
}
