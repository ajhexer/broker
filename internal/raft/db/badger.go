package db

import (
	"github.com/dgraph-io/badger/v4"
	"log"
)

type BadgerDB struct {
	Conn *badger.DB
}

func (b *BadgerDB) Init(path string) error {
	var err error
	options := badger.DefaultOptions(path)
	options.Logger = nil
	options.SyncWrites = true
	b.Conn, err = badger.Open(options)
	if err != nil {
		return err
	}
	return nil
}

func (b *BadgerDB) Close() {
	b.Conn.Tables()
	err := b.Conn.Close()
	if err != nil {
		log.Fatal(err)
	}
}
func (b *BadgerDB) Read(key []byte) ([]byte, error) {
	var value []byte

	err := b.Conn.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}

		err = item.Value(func(val []byte) error {
			value = append([]byte{}, val...)
			return nil
		})
		if err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return value, nil
}

func (b *BadgerDB) Write(key []byte, val []byte) error {
	err := b.Conn.Update(func(txn *badger.Txn) error {
		err := txn.Set(key, val)
		if err != nil {
			return err
		}
		return nil
	})

	return err
}

func (b *BadgerDB) Delete(key []byte) error {
	err := b.Conn.Update(func(txn *badger.Txn) error {
		err := txn.Delete(key)
		if err != nil {
			return err
		}
		return nil
	})

	return err
}
