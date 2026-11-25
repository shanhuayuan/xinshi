package xinshi

import "time"

type Event struct {
	Id        uint64
	TopicName string
	Type      string
	Data      any

	CreatedAt time.Time
}

type Page struct {
	Events        []Event
	EatliestId    uint64
	NextId        uint64
	NextOffset    uint64
	More          bool
	ReplayExpired bool
}
