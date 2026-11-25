package xinshi

import "context"

type Consumer interface {
	Handle(ctx context.Context, event Event) error
}
