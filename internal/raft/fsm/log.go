package fsm

type LogType int64

const (
	IncIndex LogType = 1
)

type LogData struct {
	Operation LogType
	Subject   string
	NewIndex  int32
}
