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

	Discovery qtiny.Discovery

	RemotesMutex sync.RWMutex
	Remotes      map[string]*qtiny.Nano

	LocalsMutex sync.RWMutex
	Locals      map[string]*qtiny.Nano
}

func (o *MGateway) Start(config map[string]interface{}) error {

	var configId = util.GetStr(config, "", "id")
	if len(configId) > 0 {
		o.id = configId
	}

	if len(o.id) == 0 {
		o.id = uuid.NewV4().String()
	}

	if o.Remotes == nil {
		o.Remotes = make(map[string]*qtiny.Nano)
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
	o.RemotesMutex.Lock()
	defer o.RemotesMutex.Unlock()
	if o.Remotes == nil {
		o.Remotes = make(map[string]*qtiny.Nano)
	}
	var service = o.Remotes[address]
	if service == nil {
		service = &qtiny.Nano{}
		service.Address = address
		o.Remotes[address] = service
	}
	return service
}

func (o *MGateway) RemoteGet(address string) *qtiny.Nano {
	o.RemotesMutex.RLock()
	defer o.RemotesMutex.RUnlock()
	if o.Remotes == nil {
		return nil
	}
	return o.Remotes[address]
}

func (o *MGateway) NanoLocalRegister(nano *qtiny.Nano) error {
	return nil
}

func (o *MGateway) NanoLocalUnregister(nano *qtiny.Nano) error {
	return nil
}

func (o *MGateway) NanoQuery(message *qtiny.Message) *qtiny.Nano {
	return nil
}

/* ====================================== subscribers ===================================== */

func (o *MGateway) LocalAdd(nano *qtiny.Nano) {
	o.LocalsMutex.Lock()
	defer o.LocalsMutex.Unlock()
	if o.Locals == nil {
		o.Locals = make(map[string]*qtiny.Nano)
	}

	var subscriber = o.Locals[nano.Address]
	if subscriber == nil {
		subscriber = &qtiny.Nano{}
		subscriber.Address = nano.Address
		subscriber.Options = nano.Options
		o.Locals[nano.Address] = subscriber
	}
}

func (o *MGateway) LocalRemove(address string) {
	if o.Locals == nil {
		return
	}
	o.LocalsMutex.Lock()
	defer o.LocalsMutex.Unlock()
	delete(o.Locals, address)
}

func (o *MGateway) LocalAll() map[string]*qtiny.Nano {
	if o.Locals == nil {
		return nil
	}
	var m = map[string]*qtiny.Nano{}
	o.LocalsMutex.RLock()
	defer o.LocalsMutex.RUnlock()
	for k, v := range o.Locals {
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

func (o *MGateway) GetDiscovery() qtiny.Discovery {
	return o.Discovery
}

func (o *MGateway) SetDiscovery(discovery qtiny.Discovery) {
	o.Discovery = discovery
}
