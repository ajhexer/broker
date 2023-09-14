package server

import (
	"broker/api/proto"
	"broker/internal/app"
	"broker/internal/metric"
	"broker/internal/repository"
	"broker/internal/trace"
	"broker/pkg/broker"
	"fmt"
	"google.golang.org/grpc"
	"log"
	"net"
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

	proto.RegisterBrokerServer(
		server,
		&BrokerServer{
			brokerNode: app.NewWithoutRaft(mock),
		},
	)

	log.Printf("Server starting at: %s", listener.Addr())

	if err := server.Serve(listener); err != nil {
		log.Fatalf("Server serve failed: %v", err)
	}
}
