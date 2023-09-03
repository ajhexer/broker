package fsm

import "broker/pkg/broker"

type LogType int64

const (
	DELETE LogType = 1
	SAVE           = 2
)

type LogData struct {
	Operation LogType
	Message   broker.Message
	Subject   string
}
