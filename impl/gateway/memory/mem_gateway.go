package memory

import (
	"fmt"
	"github.com/camsiabor/qcom/util"
	"github.com/camsiabor/qservice/qtiny"
	"github.com/twinj/uuid"
	"log"
	"sync"
)

type MGateway struct {
	id  string
	tag string

	Mutex sync.Mutex

	Looping bool
	Logger  *log.Logger

	QueueLimit int
	Queue      chan *qtiny.Message

	Listeners []chan *qtiny.Message

	ConsumerMutex sync.RWMutex
	Consumers     map[string]*qtiny.Nano

	SubscriberMutex sync.RWMutex
	Subscribers     map[string]*qtiny.Nano
}

func (o *MGateway) Start(config map[string]interface{}) error {

	var configId = util.GetStr(config, "", "id")
	if len(configId) > 0 {
		o.id = configId
	}

	if len(o.id) == 0 {
		o.id = uuid.NewV4().String()
	}

	if o.Consumers == nil {
		o.Consumers = make(map[string]*qtiny.Nano)
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

func (o *MGateway) Poll(limit int) (chan *qtiny.Message, error) {

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

func (o *MGateway) Post(message *qtiny.Message) error {
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

func (o *MGateway) Broadcast(message *qtiny.Message) error {
	message.Type = message.Type | qtiny.MessageTypeBroadcast
	return o.Post(message)
}

func (o *MGateway) NanoRemoteRegister(address string) *qtiny.Nano {
	o.ConsumerMutex.Lock()
	defer o.ConsumerMutex.Unlock()
	if o.Consumers == nil {
		o.Consumers = make(map[string]*qtiny.Nano)
	}
	var service = o.Consumers[address]
	if service == nil {
		service = &qtiny.Nano{}
		service.Address = address
		o.Consumers[address] = service
	}
	return service
}

func (o *MGateway) NanoRemoteGet(address string) *qtiny.Nano {
	o.ConsumerMutex.RLock()
	defer o.ConsumerMutex.RUnlock()
	if o.Consumers == nil {
		return nil
	}
	return o.Consumers[address]
}

func (o *MGateway) NanoLocalRegister(nano *qtiny.Nano) error {
	return nil
}

func (o *MGateway) NanoLocalUnregister(nano *qtiny.Nano) error {
	return nil
}

/* ====================================== subscribers ===================================== */

func (o *MGateway) SubscriberAdd(nano *qtiny.Nano) {
	o.SubscriberMutex.Lock()
	defer o.SubscriberMutex.Unlock()
	if o.Subscribers == nil {
		o.Subscribers = make(map[string]*qtiny.Nano)
	}

	var subscriber = o.Subscribers[nano.Address]
	if subscriber == nil {
		subscriber = &qtiny.Nano{}
		subscriber.Address = nano.Address
		subscriber.Options = nano.Options
		o.Subscribers[nano.Address] = subscriber
	}
}

func (o *MGateway) SubscriberRemove(address string) {
	if o.Subscribers == nil {
		return
	}
	o.SubscriberMutex.Lock()
	defer o.SubscriberMutex.Unlock()
	delete(o.Subscribers, address)
}

func (o *MGateway) GetSubscribers() map[string]*qtiny.Nano {
	if o.Subscribers == nil {
		return nil
	}
	var m = map[string]*qtiny.Nano{}
	o.SubscriberMutex.RLock()
	defer o.SubscriberMutex.RUnlock()
	for k, v := range o.Subscribers {
		m[k] = v
	}
	return m
}

/* ============================================================================================= */

func (o *MGateway) GetId() string {
	return o.id
}

func (o *MGateway) GetTag() string {
	return o.tag
}

func (o *MGateway) GetLogger() *log.Logger {
	return o.Logger
}

func (o *MGateway) SetLogger(logger *log.Logger) {
	o.Logger = logger
}
