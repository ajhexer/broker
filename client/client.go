package main

//A simple client1 to test the server

import (
	pb "broker/api/proto"
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

//type Worker struct {
//	client1 pb.BrokerClient
//}
//
//func (w Worker) run(channel chan *pb.PublishRequest, wg *sync.WaitGroup) {
//	for {
//		msg := <-channel
//		_, err := w.client1.Publish(context.Background(), msg)
//
//	}
//}

func randomString(n int) string {
	var letters = []rune("abcdefghijklmnopqrstuvwxyz")

	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}

	return string(b)
}

var concurrent bool

// var ids = make(chan int32, 5)
// var duration = time.Hour

func singlePublish(client pb.BrokerClient, subject string) (*pb.PublishResponse, error) {
	res, err := client.Publish(context.Background(), &pb.PublishRequest{
		Subject:           subject,
		Body:              []byte(randomString(16)),
		ExpirationSeconds: int32(0)})

	return res, err
}

var stime time.Time
var client1 pb.BrokerClient
var client2 pb.BrokerClient
var wg sync.WaitGroup
var count = atomic.Int64{}
var lock = sync.Mutex{}

func Publish() {

	target := func(i int) {
		defer wg.Done()
		var err error
		_, err = singlePublish(client1, "ali")

		if err == nil {
			count.Add(1)
		} else {
			log.Println(err)
		}
	}

	stime = time.Now()

	//for {
	//	time.Sleep(10 * time.Millisecond)
	//	if count.Load() > 1000 {
	//		wg.Wait()
	//		fmt.Printf("Pub count: %d, in %v time\n", count, time.Since(stime))
	//		lock.Lock()
	//		count.Store(0)
	//		lock.Unlock()
	//		stime = time.Now()
	//	}
	//
	//	wg.Add(1)
	//
	//	if concurrent {
	//		go target()
	//	} else {
	//		target()
	//	}
	//}
	for {
		count.Store(0)
		stime := time.Now()
		for i := 0; i < 10250; i++ {
			wg.Add(1)
			go target(i)
		}
		wg.Wait()
		fmt.Printf("Pub count: %d, in %v time\n", count.Load(), time.Since(stime))
	}
}

var conn *grpc.ClientConn
var url string

func newClient() (pb.BrokerClient, func()) {
	var err error

	// url := "envoy:8000"
	// url := "localhost:50051"
	// url := "192.168.70.194:31235"
	conn, err = grpc.Dial(url, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return newClient()
	}

	cancel := func() {
		conn.Close()
	}

	return pb.NewBrokerClient(conn), cancel
}

func main() {
	concurrent = os.Args[1] == "conc"
	url = os.Args[2]

	client1, _ = newClient()
	client2, _ = newClient()
	//
	//Publish()
	//count.Store(0)
	//target := func() {
	//	//defer wg.Done()
	//	res, err := singlePublish(client1, "ali")
	//	if err == nil {
	//		count.Add(1)
	//		log.Println(res.Id)
	//	} else {
	//		log.Println(err)
	//	}
	//}
	//target()
	Publish()
}
