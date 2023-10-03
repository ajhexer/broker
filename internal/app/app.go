package app

import (
	"broker/api/proto"
	"broker/internal/model"
	"broker/internal/raft/fsm"
	"broker/internal/repository"
	"broker/pkg/broker"
	"context"
	"encoding/json"
	"errors"
	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

type BrokerNode struct {
	Raft            *raft.Raft
	FSM             *fsm.BrokerFSM
	subjects        map[string]model.Subject
	IsShutdown      bool
	listenerLock    sync.RWMutex
	brokerLock      sync.Mutex
	nodeName        string
	nodeClients     map[string]*grpc.ClientConn
	grpcPort        string
	connectionsLock sync.RWMutex
	repo            repository.SecondaryDB
}

func NewModule(enableSingle bool, localID string, badgerPath string, BindAddr string, repo repository.SecondaryDB, grpcPort string) broker.Broker {
	raftInstance, fsmInstance, err := Open(enableSingle, localID, badgerPath, BindAddr)
	if err != nil {
		panic(err)
	}
	node := &BrokerNode{
		Raft:            raftInstance,
		FSM:             fsmInstance,
		subjects:        make(map[string]model.Subject),
		IsShutdown:      false,
		listenerLock:    sync.RWMutex{},
		brokerLock:      sync.Mutex{},
		nodeClients:     make(map[string]*grpc.ClientConn),
		grpcPort:        grpcPort,
		repo:            repo,
		connectionsLock: sync.RWMutex{},
		nodeName:        localID,
	}

	go node.monitorConnections()
	return node
}

func Open(enableSingle bool, localID string, badgerPath string, BindAddr string) (*raft.Raft, *fsm.BrokerFSM, error) {
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

	f, err := fsm.NewFSM(badgerPath + "/fsm")
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
		log.Println("Broker: Incrementing index")
		id, err := b.IncIndex(ctx, subject)
		if err != nil {
			return -1, err
		}
		msg.Id = int(id)
		//_, subSpan := otel.Tracer("Server").Start(ctx, "Module.putChannel")
		log.Println("Broker: Putting into channels")
		b.PutChannel(msg, subject)
		log.Println("Broker: Start Broadcast")
		b.Broadcast(msg, subject)

		//subSpan.End()
		log.Println("Broker: Saving into repo")
		err = b.repo.Save(msg, subject)
		if msg.Expiration != 0 {
			go func(msg *broker.Message, subject string) {
				<-time.After(msg.Expiration)
				b.repo.DeleteMessage(msg.Id, subject)
			}(&msg, subject)
		}

		log.Println("Broker: Published Successfully")
		return msg.Id, err
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

		msg, err := b.repo.FetchMessage(id, subject)
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

func (b *BrokerNode) Broadcast(msg broker.Message, subject string) error {

	request := &proto.PublishRequest{
		Subject: subject,
		Body:    []byte(msg.Body),
	}
	b.connectionsLock.Lock()
	log.Println("Broadcast: locked connections")
	for id, connection := range b.nodeClients {
		log.Println("Broadcast: sending to " + id)
		client := newClient(connection)
		go client.Broadcast(context.Background(), request)
	}
	log.Println("Broadcast: Unlocking")
	b.connectionsLock.Unlock()
	return nil
}

func (b *BrokerNode) IncIndex(context context.Context, subject string) (int32, error) {
	var newIndex int32
	if b.Raft.State() != raft.Leader {
		response, err := b.incIndex(context, subject)
		if err != nil {
			return -1, err
		}
		newIndex = response.NewIdx

	} else {
		index, err := b.incIndexLeader(subject)
		if err != nil {
			return -1, err
		}
		newIndex = index
	}

	return newIndex, nil
}

func (b *BrokerNode) PutChannel(message broker.Message, subject string) {
	b.listenerLock.Lock()
	for _, listener := range b.subjects[subject].Subscribers {
		if cap(listener.Channel) != len(listener.Channel) {
			listener.Channel <- message
		}
	}
	b.listenerLock.Unlock()
}

func (b *BrokerNode) getLeaderConnection() (*grpc.ClientConn, error) {
	leaderAddress, leaderId := b.Raft.LeaderWithID()
	b.connectionsLock.Lock()
	conn, ok := b.nodeClients[string(leaderId)]
	b.connectionsLock.Unlock()
	if !ok || conn.GetState() != connectivity.Ready {
		newConn, err := b.makeNewConnection(leaderAddress)
		if err != nil {
			return nil, err
		}
		b.connectionsLock.Lock()
		b.nodeClients[string(leaderId)] = newConn
		b.connectionsLock.Unlock()
		conn = newConn
	}
	return conn, nil

}

func (b *BrokerNode) incIndexLeader(subject string) (int32, error) {
	index := b.FSM.IncIndex(subject)
	logData := fsm.LogData{
		Operation: fsm.IncIndex,
		Subject:   subject,
		NewIndex:  index,
	}
	logBytes, err := json.Marshal(logData)
	if err != nil {
		return -1, err
	}
	err = b.Raft.Apply(logBytes, time.Second).Error()
	return index, err
}

func (b *BrokerNode) incIndex(globalContext context.Context, subject string) (*proto.IncIdxResponse, error) {
	conn, err := b.getLeaderConnection()
	if err != nil {
		return nil, err
	}
	client := newClient(conn)

	return client.IncIdx(globalContext, &proto.IncIdxRequest{
		Subject: subject,
	})
}

func newClient(conn *grpc.ClientConn) proto.BrokerClient {
	return proto.NewBrokerClient(conn)
}

func (b *BrokerNode) monitorConnections() {
	for {

		for _, server := range b.Raft.GetConfiguration().Configuration().Servers {
			if string(server.ID) == b.nodeName {
				continue
			}
			b.connectionsLock.Lock()
			log.Println("Monitoring: locked connections")
			if conn, ok := b.nodeClients[string(server.ID)]; !ok || conn.GetState() != connectivity.Ready {
				newConn, err := b.makeNewConnection(server.Address)
				if err != nil {
					log.Println("Monitoring: "+server.Address+" ", err)
					b.connectionsLock.Unlock()
					continue
				}
				b.nodeClients[string(server.ID)] = newConn
			}
			log.Println("Monitoring: unlocked connections")
			b.connectionsLock.Unlock()
		}
		time.Sleep(1 * time.Second)
	}
}

func (b *BrokerNode) makeNewConnection(address raft.ServerAddress) (*grpc.ClientConn, error) {
	addr := strings.Split(string(address), ":")[0]
	log.Println("Make Connection: " + addr)
	newConn, err := grpc.Dial(addr+":"+b.grpcPort, grpc.WithTransportCredentials(insecure.NewCredentials()))

	return newConn, err
}
