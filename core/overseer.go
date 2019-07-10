package core

import (
	"fmt"
	"github.com/twinj/uuid"
	"sync"
	"sync/atomic"
)

type Overseer struct {
	id string

	serial uint32

	mutex sync.RWMutex

	gateway Gateway

	queue   chan *Message
	control chan string

	waitings     []*Message
	waitingLimit uint32

	services map[string]*Service
}

func (o *Overseer) Init(gateway Gateway, waitingLimit uint32) {

	if waitingLimit <= 65536 {
		waitingLimit = 65536
	}

	o.gateway = gateway
	o.waitings = make([]*Message, waitingLimit)
	o.services = make(map[string]*Service)
}

func (o *Overseer) Start() error {

	if o.gateway == nil {
		return fmt.Errorf("gateway is null")
	}

	if len(o.id) == 0 {
		o.id = uuid.NewV4().String()
	}

	var err error
	o.queue, err = o.gateway.Poll(8192)
	if err != nil {
		return err
	}
	o.serial = 0
	o.control = make(chan string, 8)
	go o.Loop()
	return nil
}

func (o *Overseer) Stop() error {
	if o.control != nil {
		close(o.control)
		o.control = nil
	}
	return nil
}

func (o *Overseer) Loop() {
	var ok bool
	var msg *Message

	for {
		select {
		case msg, ok = <-o.queue:
			if !ok {
				break
			}
			var service = o.services[msg.Address]
			if service != nil {
				service.handle(msg)
			}
		case _, ok = <-o.control:
			if !ok {
				break
			}
		}
	}
}

func (o *Overseer) ServiceRegister(address string, options ServiceOptions, handler ServiceHandler) error {

	o.mutex.Lock()
	defer o.mutex.Unlock()

	var service = &Service{}
	service.address = address
	service.handler = handler

	var current = o.services[address]
	if current == nil {
		o.services[address] = service
	} else {
		var tail = current.tail()
		tail.next = service
		service.prev = tail
	}

	return o.gateway.ServiceRegister(address, options)

}

func (o *Overseer) ServiceUnregister(address string) error {

	o.mutex.Lock()
	defer o.mutex.Unlock()

	delete(o.services, address)

	return o.gateway.ServiceUnregister(address)
}

func (o *Overseer) generateMessageId() uint32 {
	var id = atomic.AddUint32(&o.serial, 1)
	if id >= o.waitingLimit {
		o.mutex.Lock()
		defer o.mutex.Unlock()
		if atomic.LoadUint32(&o.serial) >= o.waitingLimit {
			atomic.StoreUint32(&o.serial, 0)
		}
		id = atomic.AddUint32(&o.serial, 1)
	}
	return id
}

func (o *Overseer) Post(message *Message) (interface{}, error) {
	message.Type = SEND
	message.Sender = o.id
	if message.Timeout > 0 {
		message.ReplyId = o.generateMessageId()
		message.replyChannel = make(chan interface{})
		defer func() {
			close(message.replyChannel)
			o.waitings[message.ReplyId] = nil
		}()
	}
	var err = o.gateway.Post(message)
	if err != nil {
		return nil, err
	}
	var ret interface{}
	if message.replyChannel != nil {
		ret = <-message.replyChannel
	}
	return ret, nil
}

func (o *Overseer) Broadcast(message *Message) error {
	// TODO implement

	message.Type = BROADCAST
	return o.gateway.Post(message)

}
