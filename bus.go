package xinshi

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

type EventBus struct {
	mu sync.RWMutex

	// [topic, subscriptionId, subscription]
	subscribers map[string]map[uint64]*Subscription

	nextId atomic.Uint64
}

func NewEventBus() *EventBus {
	return &EventBus{
		subscribers: make(map[string]map[uint64]*Subscription),
	}
}

// BUG
// 发布
func (b *EventBus) Publish(topic string, event Event) (int, error) {
	if topic == "" {
		return 0, errors.New("invalid topic")
	}

	if event.CreatedAt.IsZero() {
		event.CreatedAt = time.Now()
	}

	b.mu.Lock()

	delivered, topicSubs := 0, b.subscribers[topic]
	var overflowed []*Subscription

	for idx, sub := range topicSubs {
		copyed := event
		copyed.Data = append([]byte(nil), copyed.Data...)

		select {
		case sub.queue <- copyed:
			delivered++
		default:
			delete(topicSubs, idx)
			overflowed = append(overflowed, sub)
		}
	}

	if len(topicSubs) == 0 {
		delete(b.subscribers, topic)
	}

	b.mu.Unlock()

	for _, sub := range overflowed {
		sub.cancel()
	}

	return delivered, nil
}

// 订阅
func (b *EventBus) Subscribe(topic string, bufferSize int) (*Subscription, error) {
	if topic == "" {
		return nil, errors.New("invalid topic")
	}

	bufferSize = max(bufferSize, 0)

	sub := &Subscription{
		id:    b.nextId.Add(1),
		topic: topic,
		queue: make(chan Event, bufferSize),
		done:  make(chan struct{}),
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.subscribers[topic] == nil {
		b.subscribers[topic] = make(map[uint64]*Subscription)
	}

	b.subscribers[topic][sub.id] = sub
	return sub, nil
}

func (b *EventBus) Unsubscribe(sub *Subscription) {
	if sub == nil {
		return
	}

	b.mu.Lock()

	topicSubs := b.subscribers[sub.topic]

	if topicSubs[sub.id] != sub {
		b.mu.Unlock()
		return
	}

	delete(topicSubs, sub.id)

	if len(topicSubs) == 0 {
		delete(b.subscribers, sub.topic)
	}

	b.mu.Unlock()
	sub.cancel()
}
