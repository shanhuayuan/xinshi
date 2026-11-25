package xinshi

type Config struct {
	MaxEvents           int
	MaxEventBytes       int
	SubscriberQueueSize int
}

func (c Config) normalized() Config {
	if c.SubscriberQueueSize <= 0 {
		c.SubscriberQueueSize = 256
	}
	return c
}
