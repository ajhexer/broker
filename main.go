package main

import (
	"broker/internal/app"
	"broker/internal/repository"
	"broker/pkg/broker"
	"context"
	"log"
	"math/rand"
	"sync"
	"time"
)

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
var mainCtx = context.Background()
var service1 broker.Broker

func main() {
	cassandra, err := repository.NewCassandra()
	if err != nil {
		log.Fatal(err)
	}
	service1 = app.NewModule(true, "node1", "./node1", "localhost:12001", cassandra)
	<-time.After(2 * time.Second)
	_ = app.NewModule(false, "node2", "./node2", "localhost:12002", repository.NewMock(make(map[string]map[int]broker.Message)))
	_ = app.NewModule(false, "node3", "./node3", "localhost:12003", repository.NewMock(make(map[string]map[int]broker.Message)))
	service1.Join("node2", "localhost:12002")
	service1.Join("node3", "localhost:12003")
	<-time.After(10 * time.Second)
	ticker := time.NewTicker(5000 * time.Millisecond)
	defer ticker.Stop()

	msg := createMessage()

	var wg sync.WaitGroup
	for i := 0; i < 100000; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err = service1.Publish(mainCtx, "ali", msg)
			if err != nil {
				log.Println(err.Error())
			}
			println(i)

		}(i)
	}
	wg.Wait()
	println("well done")

	select {}
}

func createMessage() broker.Message {
	body := randomString(16)

	return broker.Message{
		Body:       body,
		Expiration: 0,
	}
}
func randomString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
