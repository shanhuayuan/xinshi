package xinshi

import (
	"context"
	"sync"
)

type Subscription struct {
	id    uint64
	topic string

	queue chan Event
	done  chan struct{}

	once sync.Once
}

// 不关闭queue，只关闭done
func (s *Subscription) cancel() {
	s.once.Do(func() { close(s.done) })
}

func (s *Subscription) Consume(ctx context.Context, consumer Consumer) error {
	for {
		select {
		//接收到的被动的发来的结束信号
		case <-ctx.Done():
			return ctx.Err()
		//还没取到事件的时候接收到的主动发来的结束信号
		case <-s.done:
			return nil
		case event := <-s.queue:
			select {
			// 消费队列的时候接受到的主动发来的结束信号
			case <-s.done:
				return nil
			default:
			}

			if err := consumer.Handle(ctx, event); err != nil {
				return err
			}
		}
	}
}
