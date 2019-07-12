package memory

import (
	"fmt"
	"github.com/camsiabor/qcom/util"
	"github.com/camsiabor/qservice/qtiny"
	"github.com/twinj/uuid"
	"sync"
)

type MGateway struct {
	id  string
	tag string

	Mutex sync.Mutex

	Looping bool

	QueueLimit int
	Queue      chan *qtiny.Message

	Listeners []chan *qtiny.Message

	ServicesMutex sync.RWMutex
	Services      map[string]*qtiny.Service
}

func (o *MGateway) Start(config map[string]interface{}) error {

	var configId = util.GetStr(config, "", "id")
	if len(configId) > 0 {
		o.id = configId
	}

	if len(o.id) == 0 {
		o.id = uuid.NewV4().String()
	}

	if o.Services == nil {
		o.Services = make(map[string]*qtiny.Service)
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
	o.Queue <- message
	return nil
}

func (o *MGateway) Broadcast(message *qtiny.Message) error {
	if o.Queue == nil {
		return fmt.Errorf("gateway not started yet")
	}
	o.Queue <- message
	return nil
}

func (o *MGateway) ServiceRemoteRegister(address string, consumer string, data interface{}) {
	o.ServicesMutex.Lock()
	defer o.ServicesMutex.Unlock()
	if o.Services == nil {
		o.Services = make(map[string]*qtiny.Service)
	}
	var service = o.Services[address]
	if service == nil {
		service = &qtiny.Service{}
		service.Address = address
	}
	service.ConsumerAdd(consumer, data)
}

func (o *MGateway) ServiceRemoteGet(address string) *qtiny.Service {
	o.ServicesMutex.RLock()
	defer o.ServicesMutex.RUnlock()
	if o.Services == nil {
		return nil
	}
	return o.Services[address]
}

func (o *MGateway) ServiceRegister(address string, options qtiny.ServiceOptions) error {
	return nil
}

func (o *MGateway) ServiceUnregister(address string) error {
	return nil
}

func (o *MGateway) GetId() string {
	return o.id
}

func (o *MGateway) GetTag() string {
	return o.tag
}
