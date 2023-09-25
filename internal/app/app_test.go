package app

import (
	"broker/internal/repository"
	"broker/pkg/broker"
	"context"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"sync"
	"testing"
	"time"
)

var (
	service1 broker.Broker
	service2 broker.Broker
	service3 broker.Broker

	mainCtx = context.Background()
	letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
)

func TestMain(m *testing.M) {
	rand.Seed(time.Now().Unix())
	//postgres, err := repository.NewPostgres("postgres://postgres:postgres@localhost:5432/concurrent-http-server")
	cassandra, err := repository.NewCassandra()
	if err != nil {
		panic(err)
	}
	//service1 = NewModule(true, "node1", "./node1", "localhost:12001", repository.NewMock(make(map[string]map[int]broker.Message)))
	service1 = NewModule(true, "node1", "./node1", "localhost:12001", cassandra, "8081")
	<-time.After(2 * time.Second)
	service2 = NewModule(false, "node2", "./node2", "localhost:12002", repository.NewMock(make(map[string]map[int]broker.Message)), "8081")
	service3 = NewModule(false, "node3", "./node3", "localhost:12003", repository.NewMock(make(map[string]map[int]broker.Message)), "8081")
	service1.Join("node2", "localhost:12002")
	service1.Join("node3", "localhost:12003")
	<-time.After(10 * time.Second)
	m.Run()
}

func TestPublishShouldFailOnClosed(t *testing.T) {
	msg := createMessage()

	err := service1.Close()
	assert.Nil(t, err)

	_, err = service1.Publish(mainCtx, "ali", msg)
	assert.Equal(t, broker.ErrUnavailable, err)
}

func TestSubscribeShouldFailOnClosed(t *testing.T) {
	err := service1.Close()
	assert.Nil(t, err)

	_, err = service1.Subscribe(mainCtx, "ali")
	assert.Equal(t, broker.ErrUnavailable, err)
}

func TestFetchShouldFailOnClosed(t *testing.T) {
	err := service1.Close()
	assert.Nil(t, err)

	_, err = service1.Fetch(mainCtx, "ali", rand.Intn(100))
	assert.Equal(t, broker.ErrUnavailable, err)
}

func TestPublishShouldNotFail(t *testing.T) {
	msg := createMessage()

	_, err := service1.Publish(mainCtx, "ali", msg)

	assert.Equal(t, nil, err)
}

func TestSubscribeShouldNotFail(t *testing.T) {
	sub, err := service1.Subscribe(mainCtx, "ali")

	assert.Equal(t, nil, err)
	assert.NotEqual(t, nil, sub)
}

func TestPublishShouldSendMessageToSubscribedChan(t *testing.T) {
	msg := createMessage()

	sub, _ := service1.Subscribe(mainCtx, "ali")
	_, _ = service1.Publish(mainCtx, "ali", msg)
	in := <-sub

	assert.Equal(t, msg, in)
}

func TestPublishShouldSendMessageToSubscribedChans(t *testing.T) {
	msg := createMessage()

	sub1, _ := service1.Subscribe(mainCtx, "ali")
	sub2, _ := service1.Subscribe(mainCtx, "ali")
	sub3, _ := service1.Subscribe(mainCtx, "ali")
	_, _ = service1.Publish(mainCtx, "ali", msg)
	in1 := <-sub1
	in2 := <-sub2
	in3 := <-sub3

	assert.Equal(t, msg, in1)
	assert.Equal(t, msg, in2)
	assert.Equal(t, msg, in3)
}

func TestPublishShouldPreserveOrder(t *testing.T) {
	n := 50
	messages := make([]broker.Message, n)
	sub, _ := service1.Subscribe(mainCtx, "ali")
	for i := 0; i < n; i++ {
		messages[i] = createMessage()
		_, _ = service1.Publish(mainCtx, "ali", messages[i])
	}

	for i := 0; i < n; i++ {
		msg := <-sub
		assert.Equal(t, messages[i], msg)
	}
}

func TestPublishShouldNotSendToOtherSubscriptions(t *testing.T) {
	msg := createMessage()
	ali, _ := service1.Subscribe(mainCtx, "ali")
	maryam, _ := service1.Subscribe(mainCtx, "maryam")

	_, _ = service1.Publish(mainCtx, "ali", msg)
	select {
	case m := <-ali:
		assert.Equal(t, msg, m)
	case <-maryam:
		assert.Fail(t, "Wrong message received")
	}
}

func TestNonExpiredMessageShouldBeFetchable(t *testing.T) {
	msg := createMessageWithExpire(time.Second * 10)
	id, _ := service1.Publish(mainCtx, "ali", msg)
	<-time.After(30 * time.Millisecond)
	fMsg, _ := service1.Fetch(mainCtx, "ali", id)

	assert.Equal(t, msg, fMsg)
}

func TestExpiredMessageShouldNotBeFetchable(t *testing.T) {
	msg := createMessageWithExpire(time.Millisecond * 500)
	id, _ := service1.Publish(mainCtx, "ali", msg)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	<-ticker.C
	fMsg, err := service1.Fetch(mainCtx, "ali", id)
	assert.Equal(t, broker.ErrExpiredID, err)
	assert.Equal(t, broker.Message{}, fMsg)
}

func TestNewSubscriptionShouldNotGetPreviousMessages(t *testing.T) {
	msg := createMessage()
	_, _ = service1.Publish(mainCtx, "ali", msg)
	sub, _ := service1.Subscribe(mainCtx, "ali")

	select {
	case <-sub:
		assert.Fail(t, "Got previous message")
	default:
	}
}

func TestConcurrentSubscribesOnOneSubjectShouldNotFail(t *testing.T) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	var wg sync.WaitGroup

	for {
		select {
		case <-ticker.C:
			wg.Wait()
			return

		default:
			wg.Add(1)
			go func() {
				defer wg.Done()

				_, err := service1.Subscribe(mainCtx, "ali")
				assert.Nil(t, err)
			}()
		}
	}
}

func TestConcurrentSubscribesShouldNotFail(t *testing.T) {
	ticker := time.NewTicker(2000 * time.Millisecond)
	defer ticker.Stop()
	var wg sync.WaitGroup

	for {
		select {
		case <-ticker.C:
			wg.Wait()
			return

		default:
			wg.Add(1)
			go func() {
				defer wg.Done()

				_, err := service1.Subscribe(mainCtx, randomString(4))
				assert.Nil(t, err)
			}()
		}
	}
}

func TestConcurrentPublishOnOneSubjectShouldNotFail(t *testing.T) {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	var wg sync.WaitGroup

	msg := createMessage()

	for {
		select {
		case <-ticker.C:

			wg.Wait()
			return

		default:
			wg.Add(1)
			go func() {
				defer wg.Done()

				_, err := service1.Publish(mainCtx, "ali", msg)
				assert.Nil(t, err)
			}()
		}
	}
}

func TestConcurrentPublishShouldNotFail(t *testing.T) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	var wg sync.WaitGroup

	msg := createMessage()

	for {
		select {
		case <-ticker.C:
			wg.Wait()
			return

		default:
			wg.Add(1)
			go func() {
				defer wg.Done()

				_, err := service1.Publish(mainCtx, randomString(4), msg)
				assert.Nil(t, err)
			}()
		}
	}
}

func TestDataRace(t *testing.T) {

	duration := 500 * time.Millisecond
	ticker := time.NewTicker(duration)
	defer ticker.Stop()
	var wg sync.WaitGroup

	ids := make(chan int, 100000)

	wg.Add(1)
	go func() {
		defer wg.Done()

		for {
			select {
			case <-ticker.C:
				return

			default:
				id, err := service1.Publish(mainCtx, "ali", createMessageWithExpire(duration))
				ids <- id
				assert.Nil(t, err)
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		for {
			select {
			case <-ticker.C:
				return

			default:
				_, err := service1.Subscribe(mainCtx, "ali")
				assert.Nil(t, err)
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		for {
			select {
			case <-ticker.C:
				return

			case id := <-ids:
				_, err := service1.Fetch(mainCtx, "ali", id)
				assert.Nil(t, err)
			}
		}
	}()

	wg.Wait()
}

func BenchmarkPublish(b *testing.B) {
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := service1.Publish(mainCtx, randomString(2), createMessage())
		assert.Nil(b, err)
	}
}

func BenchmarkSubscribe(b *testing.B) {
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := service1.Subscribe(mainCtx, randomString(2))
		assert.Nil(b, err)
	}
}

func randomString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func createMessage() broker.Message {
	body := randomString(16)

	return broker.Message{
		Body:       body,
		Expiration: 0,
	}
}

func createMessageWithExpire(duration time.Duration) broker.Message {
	body := randomString(16)

	return broker.Message{
		Body:       body,
		Expiration: duration,
	}
}
