package xinshi

import "time"

type Event struct {
	Type string
	Data []byte

	CreatedAt time.Time
}
