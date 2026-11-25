package xinshi

import (
	"errors"
	"sort"
	"sync"
	"time"
)

type Topic struct {
	name       string
	config     Config
	mu         sync.Mutex
	events     []Event
	eventBytes int
	earliest   uint64
	next       uint64
	subs       map[uint64]*Subscription
	subNext    uint64
	watchers   map[uint64]chan struct{}
	watchNext  uint64
	closed     bool
}

func NewTopic(name string, config Config) *Topic {
	return &Topic{
		name:     name,
		config:   config.normalized(),
		earliest: 1,
		subs:     make(map[uint64]*Subscription),
		watchers: make(map[uint64]chan struct{}),
	}
}

func (t *Topic) trimLocked() {
	for len(t.events) > 0 && ((t.config.MaxEvents > 0 && len(t.events) > t.config.MaxEvents) || (t.config.MaxEventBytes > 0 && t.eventBytes > t.config.MaxEventBytes)) {
		t.eventBytes -= estimate(t.events[0])
		t.events[0] = Event{}
		t.events = t.events[1:]
	}
	if len(t.events) > 0 {
		t.earliest = t.events[0].Id
	} else {
		t.earliest = t.next + 1
	}
}

func (t *Topic) remove(subId uint64, expected *Subscription) {
	t.mu.Lock()
	if t.subs[subId] == expected {
		delete(t.subs, subId)
	}

	t.mu.Unlock()
	expected.abort()
}

func (t *Topic) Publish(_type string, data any) (Event, error) {
	payload, err := cloneValue(data)
	if err != nil {
		return Event{}, errors.New("clone payload failed")
	}

	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return Event{}, errors.New("topic closed")
	}
	t.next++

	event := Event{
		Id:        t.next,
		TopicName: t.name,
		Type:      _type,
		Data:      payload,
		CreatedAt: time.Now().UTC(),
	}

	size := estimate(event)

	if t.config.MaxEventBytes > 0 && size > t.config.MaxEventBytes {
		t.events = nil
		t.eventBytes = 0
		t.earliest = t.next + 1
	} else {
		t.events = append(t.events, event)
		t.eventBytes += size
		t.trimLocked()
	}

	var overflow []*Subscription
	for idx, sub := range t.subs {
		copyed, err := cloneEvent(event)
		if err != nil || !sub.enqueue(copyed) {
			delete(t.subs, idx)
			overflow = append(overflow, sub)
		}
	}

	for _, ch := range t.watchers {
		select {
		case ch <- struct{}{}:
		default:
		}
	}

	t.mu.Unlock()
	for _, s := range overflow {
		s.abort()
	}

	return cloneEvent(event)

}

func (t *Topic) Subscribe(handler Handler, offset uint64) (uint64, error) {
	if handler == nil {
		return 0, errors.New("handler is nil")
	}

	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return 0, errors.New("topic closed")
	}

	if offset == 0 {
		offset = t.earliest
	}

	if offset < t.earliest {
		t.mu.Unlock()
		return 0, errors.New("invlaid offset")
	}

	start := sort.Search(len(t.events), func(i int) bool { return t.events[i].Id >= offset })
	replay := make([]Event, 0, len(t.events)-start)
	for _, event := range t.events[start:] {
		copyed, err := cloneEvent(event)
		if err != nil {
			t.mu.Unlock()
			return 0, err
		}

		replay = append(replay, copyed)
	}

	t.subNext++
	id := t.subNext
	sub := newSubScription(handler, t.config.SubscriberQueueSize)
	t.subs[id] = sub
	t.mu.Unlock()

	for _, event := range replay {
		if sub.state.Load() == 1 || handler.Handle(event) != nil {
			t.remove(id, sub)
			return 0, errors.New("handler failed")
		}
	}

	if sub.state.Load() == 1 {
		return 0, errors.New("")
	}

	go sub.run(func() { t.remove(id, sub) })
	return id, nil
}

func (t *Topic) Unsubscribe(id uint64) {
	t.mu.Lock()
	sub := t.subs[id]
	delete(t.subs, id)
	t.mu.Unlock()
	if sub != nil {
		sub.abort()
	}
}

func (t *Topic) Watch() (<-chan struct{}, func()) {
	t.mu.Lock()
	ch := make(chan struct{}, 1)
	id := t.watchNext
	t.watchNext++
	t.watchers[id] = ch
	t.mu.Unlock()
	var once sync.Once
	return ch, func() { once.Do(func() { t.mu.Lock(); delete(t.watchers, id); t.mu.Unlock() }) }
}

func (t *Topic) Close() {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return
	}

	t.closed = true
	subs := make([]*Subscription, 0, len(t.subs))

	for _, sub := range t.subs {
		subs = append(subs, sub)
	}

	for _, ch := range t.watchers {
		select {
		case ch <- struct{}{}:
		default:
		}
	}

	t.mu.Unlock()
	for _, s := range subs {
		s.finish()
	}
}

func (t *Topic) GetEventSince(eventId uint64) []Event {
	return t.EventsPage(eventId, 0).Events
}

func (t *Topic) EventsPage(eventId uint64, limit int) Page {
	t.mu.Lock()
	defer t.mu.Unlock()

	page := Page{
		EatliestId: t.earliest,
		NextId:     t.next + 1,
		NextOffset: t.next + 1,
	}

	if eventId == 0 {
		eventId = t.earliest
	}

	if eventId < t.earliest {
		page.ReplayExpired = true
		eventId = t.earliest
	}

	start := sort.Search(len(t.events), func(i int) bool { return t.events[i].Id >= eventId })
	end := len(t.events)

	if limit > 0 && start+limit < end {
		end = start + limit
	}

	page.Events = make([]Event, 0, end-start)

	for _, event := range t.events[start:end] {
		copyed, err := cloneEvent(event)
		if err == nil {
			page.Events = append(page.Events, copyed)
		}
	}

	if len(page.Events) > 0 {
		page.NextOffset = page.Events[len(page.Events)-1].Id + 1
	}

	page.More = page.NextOffset < t.next+1
	return page
}
