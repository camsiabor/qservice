package memory

import (
	"fmt"
	"github.com/camsiabor/qcom/util"
	"github.com/camsiabor/qservice/core"
	"sync"
)

type MGateway struct {
	Mutex sync.Mutex

	Looping bool

	QueueLimit int
	Queue      chan *core.Message

	Listeners []chan *core.Message
}

func (o *MGateway) Start(config map[string]interface{}) error {

	if o.Looping {
		return fmt.Errorf("already running")
	}

	if o.Queue == nil {
		o.QueueLimit = util.GetInt(config, 8192, "queue.limit")
		if o.QueueLimit <= 16 {
			o.QueueLimit = 8192
		}
		o.Queue = make(chan *core.Message, o.QueueLimit)
	}

	o.Looping = true
	go o.Loop()
	return nil
}

func (o *MGateway) Stop(map[string]interface{}) error {
	o.Looping = false
	if o.Queue != nil {
		close(o.Queue)
		o.Queue = nil
	}
	return nil
}

func (o *MGateway) Loop() {
	var ok bool
	var msg *core.Message
	for o.Looping {
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
	o.Looping = false
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
