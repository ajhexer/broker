package app

import (
	"broker/internal/discovery"
	"broker/internal/model"
	"broker/internal/raft/fsm"
	"broker/internal/repository"
	"broker/pkg/broker"
	"context"
	"encoding/json"
	"errors"
	"github.com/hashicorp/raft"
	"go.opentelemetry.io/otel"
	"log"
	"net"
	"os"
	"sync"
	"time"
)

type BrokerNode struct {
	Raft         *raft.Raft
	FSM          *fsm.BrokerFSM
	subjects     map[string]model.Subject
	IsShutdown   bool
	listenerLock sync.RWMutex
	brokerLock   sync.Mutex
	membership   *discovery.Membership
}

func NewModule(enableSingle bool, localID string, badgerPath string, BindAddr string, repo repository.SecondaryDB) broker.Broker {
	raftInstance, fsmInstance, err := Open(enableSingle, localID, badgerPath, BindAddr, repo)
	if err != nil {
		panic(err)
	}
	return &BrokerNode{
		Raft:         raftInstance,
		FSM:          fsmInstance,
		subjects:     make(map[string]model.Subject),
		IsShutdown:   false,
		listenerLock: sync.RWMutex{},
		brokerLock:   sync.Mutex{},
	}
}

func Open(enableSingle bool, localID string, badgerPath string, BindAddr string, repo repository.SecondaryDB) (*raft.Raft, *fsm.BrokerFSM, error) {
	log.Println(enableSingle)
	log.Println("Broker Node: Opening node")

	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(localID)

	log.Println(config)

	addr, err := net.ResolveTCPAddr("tcp", BindAddr)
	if err != nil {
		log.Fatal("Broker Node: Unable to resolve TCP Bind Address")
		return nil, nil, err
	}
	log.Println(addr)
	transport, err := raft.NewTCPTransport(BindAddr, addr, 5, 2*time.Second, os.Stderr)
	if err != nil {
		log.Println(err)
		log.Fatal("Broker Node: Unable to create NewTCPTransport")
		return nil, nil, err
	}

	snapshot, err := raft.NewFileSnapshotStore(badgerPath+"/snapshot", 1, os.Stderr)
	if err != nil {
		log.Fatal("Broker Node: Unable to create SnapShot store")
		return nil, nil, err
	}

	var logStore raft.LogStore
	var stableStore raft.StableStore

	logStore = raft.NewInmemStore()

	f, err := fsm.NewFSM(badgerPath+"/fsm", repo)
	if err != nil {
		log.Fatal("Broker Node: Unable to create FSM")
		return nil, nil, err
	}

	stableStore = raft.NewInmemStore()

	r, err := raft.NewRaft(config, f, logStore, stableStore, snapshot, transport)
	if err != nil {
		log.Println(err)
		log.Fatal("Broker Node: Unable initialise raft node")

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

func (b *BrokerNode) Close() error {
	if b.IsShutdown {
		return broker.ErrUnavailable
	}

	b.IsShutdown = true

	return nil
}

func (b *BrokerNode) Publish(ctx context.Context, subject string, msg broker.Message) (int, error) {
	if b.IsShutdown {
		return -1, broker.ErrUnavailable
	}
	select {
	case <-ctx.Done():
		return -1, ctx.Err()
	default:
		_, subSpan := otel.Tracer("Server").Start(ctx, "Module.putChannel")
		msg.Id = b.FSM.IncIndex(subject)
		b.listenerLock.Lock()
		for _, listener := range b.subjects[subject].Subscribers {
			if cap(listener.Channel) != len(listener.Channel) {
				listener.Channel <- msg
			}
		}
		b.listenerLock.Unlock()
		subSpan.End()

		logData := fsm.LogData{
			Operation: fsm.SAVE,
			Message:   msg,
			Subject:   subject,
		}
		logBytes, err := json.Marshal(logData)
		if err != nil {
			return -1, err
		}
		_, applySpan := otel.Tracer("Server").Start(ctx, "Module.raftApply")
		defer applySpan.End()
		b.brokerLock.Lock()
		f := b.Raft.Apply(logBytes, time.Second)
		b.brokerLock.Unlock()
		if msg.Expiration != 0 {
			go func(msg *broker.Message, subject string) {
				<-time.After(msg.Expiration)
				logData := fsm.LogData{
					Operation: fsm.DELETE,
					Message:   *msg,
					Subject:   subject,
				}
				logBytes, err := json.Marshal(logData)
				if err != nil {
					panic(err)
				}
				b.Raft.Apply(logBytes, time.Millisecond)
			}(&msg, subject)
		}

		return msg.Id, f.Error()
	}
}

func (b *BrokerNode) Subscribe(ctx context.Context, subject string) (<-chan broker.Message, error) {
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

func (b *BrokerNode) Fetch(ctx context.Context, subject string, id int) (broker.Message, error) {
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
func (b *BrokerNode) Join(nodeID, raftAddr string) error {
	log.Printf("received join request for remote node %s, raftAddr %s\n", nodeID, raftAddr)
	if b.Raft.State() != raft.Leader {
		return errors.New("node is not leader")
	}
	config := b.Raft.GetConfiguration()
	if err := config.Error(); err != nil {
		log.Printf("failed to get raft configuration\n")
		return err
	}
	for _, server := range config.Configuration().Servers {
		if server.ID == raft.ServerID(nodeID) {
			log.Printf("node %s already joined raft cluster\n", nodeID)
			return errors.New("node already exists")
		}
	}

	f := b.Raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(raftAddr), 0, 0)
	if err := f.Error(); err != nil {
		return err
	}
	log.Printf("node %s at %s joined successfully\n", nodeID, raftAddr)
	return nil
}

func (b *BrokerNode) Leave(nodeID string) error {
	log.Printf("received leave request for remote node %s", nodeID)
	if b.Raft.State() != raft.Leader {
		return errors.New("node is not leader")
	}

	config := b.Raft.GetConfiguration()

	if err := config.Error(); err != nil {
		log.Printf("failed to get raft configuration\n")
		return err
	}

	for _, server := range config.Configuration().Servers {
		if server.ID == raft.ServerID(nodeID) {
			f := b.Raft.RemoveServer(server.ID, 0, 0)
			if err := f.Error(); err != nil {
				log.Printf("failed to remove server %s\n", nodeID)
				return err
			}

			log.Printf("node %s left successfully\n", nodeID)
			return nil
		}
	}

	log.Printf("node %s not exist in raft group\n", nodeID)
	return errors.New("Node doesnt exist in the cluster")
}
