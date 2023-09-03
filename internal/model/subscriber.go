package model

import "broker/pkg/broker"

type Subscriber struct {
	Channel chan broker.Message
}
