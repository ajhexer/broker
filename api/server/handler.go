package server

import (
	"broker/api/proto"
	"broker/internal/app"
	"broker/internal/discovery"
	"broker/internal/metric"
	"broker/pkg/broker"
	"context"
	"errors"
	"fmt"
	"github.com/hashicorp/raft"
	"go.opentelemetry.io/otel"
	"log"
	"sync"
	"time"
)

type BrokerServer struct {
	proto.UnimplementedBrokerServer
	brokerNode broker.Broker
	membership *discovery.Membership
}

func NewBrokerServer(brokerNode broker.Broker, config discovery.Config) (*BrokerServer, error) {
	membership, err := discovery.New(brokerNode, config)
	if err != nil {
		return nil, err
	}
	return &BrokerServer{
		brokerNode: brokerNode,
		membership: membership,
	}, nil
}

func (s *BrokerServer) Publish(globalContext context.Context, request *proto.PublishRequest) (*proto.PublishResponse, error) {
	log.Println("Received publish message")
	publishStartTime := time.Now()

	msg := broker.Message{
		Body:       (string)(request.Body),
		Expiration: time.Duration(request.ExpirationSeconds) * time.Second,
	}

	publishId, err := s.brokerNode.Publish(globalContext, request.Subject, msg)
	go s.Broadcast(globalContext, request)

	publishDuration := time.Since(publishStartTime).Seconds()
	metric.MethodDuration.WithLabelValues("publish_duration").Observe(publishDuration)

	if err != nil {
		metric.MethodCount.WithLabelValues("publish", "failed").Inc()

		return nil, err
	}

	metric.MethodCount.WithLabelValues("publish", "successful").Inc()

	//globalSpan.End()

	log.Println("published message")

	return &proto.PublishResponse{Id: int32(publishId)}, nil
}

func (s *BrokerServer) Subscribe(request *proto.SubscribeRequest, server proto.Broker_SubscribeServer) error {
	fmt.Println("Subscriber request received.")

	var subscribeError error

	SubscribedChannel, err := s.brokerNode.Subscribe(
		server.Context(),
		request.Subject,
	)

	if err != nil {
		metric.MethodCount.WithLabelValues("subscribe", "failed").Inc()
		return err
	}

	metric.ActiveSubscribersGauge.Inc()

	ctx := server.Context()

	var wg sync.WaitGroup

	wg.Add(1)

	go func() {
		for {
			select {
			case msg, ok := <-SubscribedChannel:
				if !ok {
					metric.ActiveSubscribersGauge.Dec()
					wg.Done()

					return
				}

				if err := server.Send(&(proto.MessageResponse{Body: []byte(msg.Body)})); err != nil {
					subscribeError = err
				}
			case <-ctx.Done():
				subscribeError = errors.New("context timeout reached")

				metric.ActiveSubscribersGauge.Dec()
				wg.Done()

				return
			}
		}
	}()

	wg.Wait()

	if subscribeError == nil {
		metric.MethodCount.WithLabelValues("subscribe", "successful").Inc()
	} else {
		metric.MethodCount.WithLabelValues("subscribe", "failed").Inc()
	}

	return subscribeError
}

func (s *BrokerServer) Fetch(ctx context.Context, request *proto.FetchRequest) (*proto.MessageResponse, error) {
	fetchStartTime := time.Now()

	log.Println("Getting fetch request")
	defer log.Println("Finish handling fetch request")

	_, moduleFetch := otel.Tracer("Server").Start(ctx, "Get or create topics by name")
	msg, err := s.brokerNode.Fetch(ctx, request.Subject, int(request.Id))
	moduleFetch.End()

	if err != nil {
		metric.MethodCount.WithLabelValues("fetch", "failed").Inc()

		return nil, err
	}

	fetchDuration := time.Since(fetchStartTime)
	metric.MethodDuration.WithLabelValues("fetch_duration").Observe(float64(fetchDuration) / float64(time.Nanosecond))
	metric.MethodCount.WithLabelValues("fetch", "successful").Inc()

	return &proto.MessageResponse{Body: []byte(msg.Body)}, nil
}

func (s *BrokerServer) Broadcast(_ context.Context, request *proto.PublishRequest) (*proto.BroadcastResponse, error) {
	s.brokerNode.PutChannel(broker.Message{Body: string(request.Body)}, request.Subject)
	return &proto.BroadcastResponse{
		Ack: true,
	}, nil
}

func (s *BrokerServer) IncIdx(context context.Context, request *proto.IncIdxRequest) (*proto.IncIdxResponse, error) {
	if s.brokerNode.(*app.BrokerNode).Raft.State() != raft.Leader {
		return nil, errors.New("node is not leader")
	}
	newIndex, err := s.brokerNode.IncIndex(context, request.Subject)
	if err != nil {
		return nil, err
	}
	response := &proto.IncIdxResponse{
		NewIdx: newIndex,
	}
	return response, nil
}
