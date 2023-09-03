package fsm

import (
	"broker/internal/raft/db"
	"github.com/hashicorp/raft"
)

type BrokerSnapshot struct {
	db *db.BadgerDB
}

func (s *BrokerSnapshot) Persist(sink raft.SnapshotSink) error {
	//TODO: add logging

	_, err := s.db.Conn.Backup(sink, 0)
	if err != nil {

		return err
	}

	err = sink.Close()
	if err != nil {

		return err
	}

	return nil
}

func (s *BrokerSnapshot) Release() {
	// TODO: add logging
}
