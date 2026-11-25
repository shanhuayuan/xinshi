package xinshi

import (
	"sync/atomic"
)

type Handler interface {
	Handle(Event) error
}

type Subscription struct {
	handler Handler
	queue   chan Event
	stop    chan struct{}
	state   atomic.Uint32
}

func newSubScription(handler Handler, size int) *Subscription {
	return &Subscription{handler: handler, queue: make(chan Event, size), stop: make(chan struct{})}
}

func (s *Subscription) enqueue(event Event) bool {
	if s.state.Load() != 0 {
		return false
	}

	select {
	case s.queue <- event:
		return true
	case <-s.stop:
		return false
	default:
		return false
	}
}

func (s *Subscription) abort() {
	if s.state.Swap(1) != 1 {
		close(s.stop)
	}
}

func (s *Subscription) finish() {
	if s.state.CompareAndSwap(0, 2) {
		close(s.queue)
	}
}

func (s *Subscription) run(onError func()) {
	for {
		select {
		case <-s.stop:
			return
		case event, ok := <-s.queue:
			if !ok {
				return
			}

			if s.state.Load() == 1 {
				return
			}

			if err := s.handler.Handle(event); err != nil {
				onError()
				return
			}
		}
	}
}
