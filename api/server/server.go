package server

import (
	"broker/api/proto"
	"broker/internal/app"
	"broker/internal/discovery"
	"broker/internal/metric"
	"broker/internal/repository"
	"broker/internal/trace"
	"fmt"
	"google.golang.org/grpc"
	"log"
	"net"
	"os"
	"time"
)

var (
	Port = "8081"
)

func Init() {
	go metric.StartPrometheusServer()
	trace.Register()
}

func Run() {
	Init()

	listener, err := net.Listen("tcp", fmt.Sprintf(":%s", Port))
	if err != nil {
		log.Fatalf("Cant start listener: %v", err)
	}

	server := grpc.NewServer()

	postgres, err := repository.NewPostgres()
	if err != nil {
		log.Fatalln(err)
	}

	hostname, found := os.LookupEnv("HOSTNAME")
	if !found {
		panic("HOSTNAME environment variable not set.")
	}
	kubernetes := discovery.NewKubernetesDiscovery("default", hostname)

	startIPAddresses, err, localIP := kubernetes.GetIPAddresses()
	if err != nil {
		panic(err)
	}
	enableSingle := string(hostname[len(hostname)-1]) == "0"
	raftBind := localIP + ":12000"
	serfBind := localIP + ":12001"
	brokerNode := app.NewModule(enableSingle, hostname, "./storage", raftBind, postgres, Port)
	time.Sleep(5 * time.Second)

	brokerServer, err := NewBrokerServer(brokerNode, discovery.Config{
		NodeName: hostname,
		BindAddr: serfBind,
		Tags: map[string]string{
			"rpc_addr": raftBind,
		},
		StartJoinAddresses: startIPAddresses,
	})
	if err != nil {
		log.Fatalln(err)
	}

	proto.RegisterBrokerServer(
		server,
		brokerServer,
	)

	log.Printf("Server starting at: %s", listener.Addr())

	if err := server.Serve(listener); err != nil {
		log.Fatalf("Server serve failed: %v", err)
	}
}
