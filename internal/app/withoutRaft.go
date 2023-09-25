package app

import (
	"broker/internal/model"
	"broker/internal/repository"
	"broker/pkg/broker"
	"context"
	"go.opentelemetry.io/otel"
	"sync"
	"time"
)

type BrokerNodeWithoutRaft struct {
	repo         repository.SecondaryDB
	subjects     map[string]model.Subject
	IsShutdown   bool
	listenerLock sync.RWMutex
	brokerLock   sync.Mutex
	lastIndexes  map[string]int
}

func NewWithoutRaft(repo repository.SecondaryDB) broker.Broker {

	return nil
}

func (b *BrokerNodeWithoutRaft) Close() error {
	if b.IsShutdown {
		return broker.ErrUnavailable
	}

	b.IsShutdown = true

	return nil
}

func (b *BrokerNodeWithoutRaft) Publish(ctx context.Context, subject string, msg broker.Message) (int, error) {
	if b.IsShutdown {
		return -1, broker.ErrUnavailable
	}
	select {
	case <-ctx.Done():
		return -1, ctx.Err()
	default:
		_, subSpan := otel.Tracer("Server").Start(ctx, "Module.putChannel")
		msg.Id = b.IncIndex(subject)
		b.listenerLock.Lock()
		for _, listener := range b.subjects[subject].Subscribers {
			if cap(listener.Channel) != len(listener.Channel) {
				listener.Channel <- msg
			}
		}
		b.listenerLock.Unlock()
		subSpan.End()
		_, applySpan := otel.Tracer("Server").Start(ctx, "Module.raftApply")
		defer applySpan.End()

		err := b.repo.Save(msg, subject)
		if msg.Expiration != 0 {
			go func(msg *broker.Message, subject string) {
				<-time.After(msg.Expiration)

				err := b.repo.DeleteMessage(msg.Id, subject)
				if err != nil {
					panic(err)
				}
			}(&msg, subject)
		}

		return msg.Id, err
	}
}

func (b *BrokerNodeWithoutRaft) Subscribe(ctx context.Context, subject string) (<-chan broker.Message, error) {
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

func (b *BrokerNodeWithoutRaft) Fetch(ctx context.Context, subject string, id int) (broker.Message, error) {
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
func (b *BrokerNodeWithoutRaft) Join(nodeID, raftAddr string) error {
	panic("implement me")
}

func (b *BrokerNodeWithoutRaft) Leave(nodeID string) error {
	panic("implement me")
}

func (b *BrokerNodeWithoutRaft) IncIndex(subject string) int {
	var index int
	b.brokerLock.Lock()
	if _, ok := b.lastIndexes[subject]; !ok {
		b.lastIndexes[subject] = 0
	}
	index = b.lastIndexes[subject]
	b.lastIndexes[subject]++
	b.brokerLock.Unlock()
	return index
}
