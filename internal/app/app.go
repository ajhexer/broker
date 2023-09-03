package app

import (
	"broker/internal/model"
	"broker/internal/raft/fsm"
	"broker/internal/raft/repository"
	"broker/internal/raft/store"
	"broker/pkg/broker"
	"context"
	"encoding/json"
	"github.com/hashicorp/raft"
	"log"
	"net"
	"os"
	"sync"
	"time"
)

type BrokerImpl struct {
	Raft         *raft.Raft
	FSM          *fsm.BrokerFSM
	subjects     map[string]model.Subject
	IsShutdown   bool
	listenerLock sync.RWMutex
}

func NewModule(enableSingle bool, localID string, badgerPath string, BindAddr string, repo repository.SecondaryDB) broker.Broker {
	raftInstance, fsmInstance, err := Open(enableSingle, localID, badgerPath, BindAddr, repo)
	if err != nil {
		panic(err)
	}

	return &BrokerImpl{
		Raft:         raftInstance,
		FSM:          fsmInstance,
		subjects:     make(map[string]model.Subject),
		IsShutdown:   false,
		listenerLock: sync.RWMutex{},
	}
}

func Open(enableSingle bool, localID string, badgerPath string, BindAddr string, repo repository.SecondaryDB) (*raft.Raft, *fsm.BrokerFSM, error) {
	log.Println(enableSingle)
	log.Println("RavelNode: Opening node")

	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(localID)

	log.Println(config)

	addr, err := net.ResolveTCPAddr("tcp", BindAddr)
	if err != nil {
		log.Fatal("RavelNode: Unable to resolve TCP Bind Address")
		return nil, nil, err
	}
	log.Println(addr)
	transport, err := raft.NewTCPTransport(BindAddr, addr, 5, 2*time.Second, os.Stderr)
	if err != nil {
		log.Println(err)
		log.Fatal("RavelNode: Unable to create NewTCPTransport")
		return nil, nil, err
	}

	snapshot, err := raft.NewFileSnapshotStore(badgerPath+"/snapshot", 1, os.Stderr)
	if err != nil {
		log.Fatal("RavelNode: Unable to create SnapShot store")
		return nil, nil, err
	}

	var logStore raft.LogStore
	var stableStore raft.StableStore

	logStore, err = store.NewRavelLogStore(badgerPath + "/logs")
	if err != nil {
		log.Fatal("RavelNode: Unable to create Log store")
		return nil, nil, err
	}

	f, err := fsm.NewFSM(badgerPath+"/fsm", repo)
	if err != nil {
		log.Fatal("RavelNode: Unable to create FSM")
		return nil, nil, err
	}

	stableStore, err = store.NewRavelStableStore(badgerPath + "/stable")
	if err != nil {
		log.Fatal("RavelNode: Unable to create Stable store")
		return nil, nil, err
	}

	r, err := raft.NewRaft(config, f, logStore, stableStore, snapshot, transport)
	if err != nil {
		log.Println(err)
		log.Fatal("RavelNode: Unable initialise raft node")

		return nil, nil, err
	}

	if enableSingle {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		r.BootstrapCluster(configuration)
	}

	return r, f, nil
}

func (b *BrokerImpl) Close() error {
	if b.IsShutdown {
		return broker.ErrUnavailable
	}

	b.IsShutdown = true

	return nil
}

func (b *BrokerImpl) Publish(ctx context.Context, subject string, msg broker.Message) (int, error) {
	if b.IsShutdown {
		return -1, broker.ErrUnavailable
	}
	select {
	case <-ctx.Done():
		return -1, ctx.Err()
	default:
		b.listenerLock.Lock()
		for _, listener := range b.subjects[subject].Subscribers {
			if cap(listener.Channel) != len(listener.Channel) {
				listener.Channel <- msg
			}
		}
		b.listenerLock.Unlock()
		msg.Id = b.FSM.IncIndex(subject)
		logData := fsm.LogData{
			Operation: fsm.SAVE,
			Message:   msg,
			Subject:   subject,
		}
		logBytes, err := json.Marshal(logData)
		if err != nil {
			return -1, err
		}

		f := b.Raft.Apply(logBytes, time.Second)
		//saveSpan.End()

		if msg.Expiration != 0 {
			go func(msg *broker.Message, subject string) {
				<-time.After(msg.Expiration)
				b.listenerLock.Lock()
				logData := fsm.LogData{
					Operation: fsm.DELETE,
					Message:   *msg,
					Subject:   subject,
				}
				logBytes, err := json.Marshal(logData)
				if err != nil {
					panic(err)
				}
				b.Raft.Apply(logBytes, time.Second)
				b.listenerLock.Unlock()
			}(&msg, subject)
		}

		return msg.Id, f.Error()
	}
}

func (b *BrokerImpl) Subscribe(ctx context.Context, subject string) (<-chan broker.Message, error) {
	if b.IsShutdown {
		return nil, broker.ErrUnavailable
	}

	select {
	case <-ctx.Done():
		return nil, broker.ErrInvalidID
	default:
		newChannel := make(chan broker.Message, 100)

		b.listenerLock.Lock()
		b.subjects[subject] = model.Subject{
			Subscribers: append(b.subjects[subject].Subscribers, model.Subscriber{Channel: newChannel}),
		}
		b.listenerLock.Unlock()

		return newChannel, nil
	}
}

func (b *BrokerImpl) Fetch(ctx context.Context, subject string, id int) (broker.Message, error) {
	if b.IsShutdown {
		return broker.Message{}, broker.ErrUnavailable
	}

	select {
	case <-ctx.Done():
		return broker.Message{}, broker.ErrInvalidID
	default:

		msg, err := b.FSM.Get(subject, id)
		if err != nil {
			return broker.Message{}, broker.ErrExpiredID
		}

		return msg, nil
	}
}
