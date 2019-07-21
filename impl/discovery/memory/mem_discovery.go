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

	o.Looping = true
	return nil
}

func (o *MGateway) Stop(map[string]interface{}) error {
	o.Looping = false
	return nil
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
