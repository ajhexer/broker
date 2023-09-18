package server

import (
	"broker/api/proto"
	"broker/internal/app"
	"broker/internal/discovery"
	"broker/internal/metric"
	"broker/internal/repository"
	"broker/internal/trace"
	"broker/pkg/broker"
	"fmt"
	"google.golang.org/grpc"
	"log"
	"net"
	"os"
)

var (
	Port = 8081
)

func Init() {
	go metric.StartPrometheusServer()
	trace.Register()
}

func Run() {
	Init()

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", Port))
	if err != nil {
		log.Fatalf("Cant start listener: %v", err)
	}

	server := grpc.NewServer()
	//cassandra, err := repository.NewCassandra()
	//postgres, err := repository.NewPostgres()
	//cassandra, err := repository.GetCassandra()
	mock := repository.NewMock(make(map[string]map[int]broker.Message))

	//if err != nil {
	//	panic(err)
	//}
	hostname, found := os.LookupEnv("HOSTNAME")
	if !found {
		panic("HOSTNAME environment variable not set.")
	}
	kubernetes := discovery.NewKubernetesDiscovery("default", hostname)

	startIPAddresses, err, localIP := kubernetes.GetIPAddresses()
	if err != nil {
		panic(err)
	}
	enableSingle := len(startIPAddresses) == 0
	raftBind := localIP + ":12000"
	serfBind := localIP + ":12001"
	brokerNode := app.NewModule(enableSingle, hostname, "./"+hostname, raftBind, mock)
	_, err = discovery.New(brokerNode, discovery.Config{
		NodeName: hostname,
		BindAddr: serfBind,
		Tags: map[string]string{
			"rpc_addr": raftBind,
		},
		StartJoinAddresses: startIPAddresses,
	})

	proto.RegisterBrokerServer(
		server,
		&BrokerServer{
			brokerNode: brokerNode,
		},
	)

	log.Printf("Server starting at: %s", listener.Addr())

	if err := server.Serve(listener); err != nil {
		log.Fatalf("Server serve failed: %v", err)
	}
}
