package memory

import (
	"fmt"
	"github.com/camsiabor/qcom/util"
	"github.com/camsiabor/qservice/qtiny"
	"github.com/twinj/uuid"
	"log"
	"sync"
)

type MemGateway struct {
	id  string
	tag string

	Mutex sync.Mutex

	Looping bool
	Logger  *log.Logger

	QueueLimit int
	Queue      chan *qtiny.Message

	Listeners []chan *qtiny.Message

	Discovery qtiny.Discovery
}

func (o *MemGateway) Start(config map[string]interface{}) error {

	var configId = util.GetStr(config, "", "id")
	if len(configId) > 0 {
		o.id = configId
	}

	if len(o.id) == 0 {
		o.id = uuid.NewV4().String()
	}

	if o.Looping {
		return fmt.Errorf("already running")
	}

	if o.Queue == nil {
		o.QueueLimit = util.GetInt(config, 8192, "queue.limit")
		if o.QueueLimit <= 16 {
			o.QueueLimit = 8192
		}
		o.Queue = make(chan *qtiny.Message, o.QueueLimit)
	}

	o.Looping = true
	go o.Loop()
	return nil
}

func (o *MemGateway) Stop(map[string]interface{}) error {
	o.Looping = false
	if o.Queue != nil {
		close(o.Queue)
		o.Queue = nil
	}
	return nil
}

func (o *MemGateway) Loop() {
	var ok bool
	var msg *qtiny.Message
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

func (o *MemGateway) Poll(limit int) (chan *qtiny.Message, error) {

	if limit <= 0 {
		limit = 8192
	}

	var ch = make(chan *qtiny.Message, limit)

	o.Mutex.Lock()
	defer o.Mutex.Unlock()

	if o.Listeners == nil {
		o.Listeners = make([]chan *qtiny.Message, 1)
		o.Listeners[0] = ch
	} else {
		o.Listeners = append(o.Listeners, ch)
	}

	return ch, nil
}

func (o *MemGateway) Post(message *qtiny.Message) error {
	if o.Queue == nil {
		return fmt.Errorf("gateway not started yet")
	}
	if message.Type&qtiny.MessageTypeReply > 0 {
		message.Address = message.Sender
	}
	message.Sender = o.id
	message = message.Clone()
	o.Queue <- message
	return nil
}

func (o *MemGateway) Broadcast(message *qtiny.Message) error {
	message.Type = message.Type | qtiny.MessageTypeBroadcast
	return o.Post(message)
}

/* ============================================================================================= */

func (o *MemGateway) GetId() string {
	return o.id
}

func (o *MemGateway) SetId(id string) {
	o.id = id
}

func (o *MemGateway) GetTag() string {
	return o.tag
}

func (o *MemGateway) GetLogger() *log.Logger {
	return o.Logger
}

func (o *MemGateway) SetLogger(logger *log.Logger) {
	o.Logger = logger
}

func (o *MemGateway) GetDiscovery() qtiny.Discovery {
	return o.Discovery
}

func (o *MemGateway) SetDiscovery(discovery qtiny.Discovery) {
	o.Discovery = discovery
}
