package xinshi

import (
	"errors"
	"sync"
)

type Bus struct {
	mu     sync.RWMutex
	config Config
	topics map[string]*Topic
	closed bool
}

func NewBus(config Config) *Bus {
	return &Bus{config: config.normalized(), topics: make(map[string]*Topic)}
}

func (b *Bus) GetTopic(name string) (*Topic, error) {
	if name == "" {
		return nil, errors.New("invalid topic name")
	}

	b.mu.RLock()
	t := b.topics[name]
	closed := b.closed
	b.mu.RUnlock()

	if t != nil {
		return t, nil
	}

	if closed {
		return nil, errors.New("cloesed")
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return nil, errors.New("closed")
	}

	if t = b.topics[name]; t == nil {
		t = NewTopic(name, b.config)
		b.topics[name] = t
	}

	return t, nil
}

func (b *Bus) Publish(name, _type string, data any) (Event, error) {
	t, err := b.GetTopic(name)
	if err != nil {
		return Event{}, err
	}

	return t.Publish(_type, data)
}

func (b *Bus) Close() {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return
	}

	b.closed = true
	topics := make([]*Topic, 0, len(b.topics))

	for _, t := range topics {
		topics = append(topics, t)
	}

	b.mu.Unlock()
	for _, t := range topics {
		t.Close()
	}
}
