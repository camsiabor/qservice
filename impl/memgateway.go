package impl

import (
	"fmt"
	"github.com/camsiabor/qservice/core"
	"sync"
)

type MemoryGateway struct {
	QueueLimit int
	mutex      sync.Mutex
	queue      chan *core.Message
	listeners  []chan *core.Message
}

func (o *MemoryGateway) Start() error {
	if o.queue != nil {
		return fmt.Errorf("already started")
	}
	if o.QueueLimit <= 16 {
		o.QueueLimit = 8192
	}
	o.queue = make(chan *core.Message, o.QueueLimit)
	go o.Loop()
	return nil
}

func (o *MemoryGateway) Stop() error {
	if o.queue != nil {
		close(o.queue)
		o.queue = nil
	}
	return nil
}

func (o *MemoryGateway) Loop() {
	var ok bool
	var msg *core.Message
	for {
		select {
		case msg, ok = <-o.queue:
			if !ok {
				break
			}
			if o.listeners == nil {
				continue
			}
		}
		if msg != nil {
			var n = len(o.listeners)
			for i := 0; i < n; i++ {
				o.listeners[i] <- msg
			}
		}
	}

}

func (o *MemoryGateway) Poll(limit int) (chan *core.Message, error) {

	if limit <= 0 {
		limit = 8192
	}

	var ch = make(chan *core.Message, limit)

	o.mutex.Lock()
	defer o.mutex.Unlock()

	if o.listeners == nil {
		o.listeners = make([]chan *core.Message, 1)
		o.listeners[0] = ch
	} else {
		o.listeners = append(o.listeners, ch)
	}

	return ch, nil
}

func (o *MemoryGateway) Post(message *core.Message) error {
	if o.queue == nil {
		return fmt.Errorf("gateway not started yet")
	}
	o.queue <- message
	return nil
}

func (o *MemoryGateway) Broadcast(message *core.Message) error {
	if o.queue == nil {
		return fmt.Errorf("gateway not started yet")
	}
	o.queue <- message
	return nil
}

func (o MemoryGateway) ServiceRegister(address string, options core.ServiceOptions) error {
	return nil
}

func (o MemoryGateway) ServiceUnregister(address string) error {
	return nil
}
