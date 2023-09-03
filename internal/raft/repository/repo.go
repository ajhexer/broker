package repository

import (
	"broker/pkg/broker"
	"errors"
	"sync"
)

type SecondaryDB interface {
	Save(msg broker.Message, subject string)
	FetchMessage(id int, subject string) (broker.Message, error)
	DeleteMessage(id int, subject string) error
	DropAll() error
}

type MockDB struct {
	lock     sync.Mutex
	messages map[string]map[int]broker.Message
}

func NewMock(messages map[string]map[int]broker.Message) *MockDB {
	return &MockDB{
		messages: messages,
	}
}

func (m *MockDB) Save(msg broker.Message, subject string) {
	m.lock.Lock()
	if _, ok := m.messages[subject]; !ok {
		m.messages[subject] = make(map[int]broker.Message)
	}

	m.messages[subject][msg.Id] = msg
	m.lock.Unlock()
}

func (m *MockDB) FetchMessage(id int, subject string) (broker.Message, error) {
	m.lock.Lock()
	defer m.lock.Unlock()
	if msg, ok := m.messages[subject][id]; ok {
		return msg, nil
	}
	return broker.Message{}, errors.New("can't find the message")
}

func (m *MockDB) DeleteMessage(id int, subject string) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	if _, ok := m.messages[subject][id]; ok {
		delete(m.messages[subject], id)
		return nil
	}
	return errors.New("can't find the message")
}

func (m *MockDB) DropAll() error {
	m.lock.Lock()
	m.messages = make(map[string]map[int]broker.Message)
	m.lock.Unlock()
	return nil
}
