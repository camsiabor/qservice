package memory

import (
	"fmt"
	"github.com/camsiabor/qservice/core"
	"sync"
)

type MGateway struct {
	QueueLimit int
	Mutex      sync.Mutex
	Queue      chan *core.Message
	Listeners  []chan *core.Message
}

func (o *MGateway) Start(...interface{}) error {
	if o.Queue != nil {
		return fmt.Errorf("already started")
	}
	if o.QueueLimit <= 16 {
		o.QueueLimit = 8192
	}
	o.Queue = make(chan *core.Message, o.QueueLimit)
	go o.Loop()
	return nil
}

func (o *MGateway) Stop(...interface{}) error {
	if o.Queue != nil {
		close(o.Queue)
		o.Queue = nil
	}
	return nil
}

func (o *MGateway) Loop() {
	var ok bool
	var msg *core.Message
	for {
		select {
		case msg, ok = <-o.Queue:
			if !ok {
				break
			}
			if o.Listeners == nil {
				continue
			}
		}
		if msg != nil {
			var n = len(o.Listeners)
			for i := 0; i < n; i++ {
				o.Listeners[i] <- msg
			}
		}
	}

}

func (o *MGateway) Poll(limit int) (chan *core.Message, error) {

	if limit <= 0 {
		limit = 8192
	}

	var ch = make(chan *core.Message, limit)

	o.Mutex.Lock()
	defer o.Mutex.Unlock()

	if o.Listeners == nil {
		o.Listeners = make([]chan *core.Message, 1)
		o.Listeners[0] = ch
	} else {
		o.Listeners = append(o.Listeners, ch)
	}

	return ch, nil
}

func (o *MGateway) Post(message *core.Message) error {
	if o.Queue == nil {
		return fmt.Errorf("gateway not started yet")
	}
	o.Queue <- message
	return nil
}

func (o *MGateway) Broadcast(message *core.Message) error {
	if o.Queue == nil {
		return fmt.Errorf("gateway not started yet")
	}
	o.Queue <- message
	return nil
}

func (o MGateway) ServiceRegister(address string, options core.ServiceOptions) error {
	return nil
}

func (o MGateway) ServiceUnregister(address string) error {
	return nil
}
