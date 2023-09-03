package broker

import (
	"context"
	"io"
	"time"
)

type Message struct {
	Id         int
	Body       string
	Expiration time.Duration
}

type Broker interface {
	io.Closer

	Publish(ctx context.Context, subject string, msg Message) (int, error)

	Subscribe(ctx context.Context, subject string) (<-chan Message, error)

	Fetch(ctx context.Context, subject string, id int) (Message, error)
}
