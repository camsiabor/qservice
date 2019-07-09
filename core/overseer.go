package core

import (
	"fmt"
	"math/rand"
	"sync"
)

type Overseer struct {
	serial int64

	mutex sync.RWMutex

	gateway  Gateway
	services map[string]*Service

	queue   chan *Message
	control chan string
}

func (o *Overseer) Init(gateway Gateway) {
	o.gateway = gateway
	o.services = make(map[string]*Service)
}

func (o *Overseer) Start() error {
	if o.gateway == nil {
		return fmt.Errorf("gateway is null")
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

	var current = o.services[address]
	if current == nil {
		o.services[address] = service
	} else {
		current.tail().next = service
	}

	return o.gateway.ServiceRegister(address, options)

}

func (o *Overseer) ServiceUnregister(address string) error {

	o.mutex.Lock()
	defer o.mutex.Unlock()

	delete(o.services, address)

	return o.gateway.ServiceUnregister(address)
}

func (o *Overseer) Post(address string, data interface{}, headers MessageHeaders, options MessageOptions) error {
	var message = &Message{}
	message.Id = rand.Int63()
	message.Address = address
	message.Data = data
	message.Headers = headers
	message.Options = options
	return o.gateway.Post(message)
}

func (o *Overseer) Broadcast(address string, data interface{}, headers MessageHeaders, options MessageOptions) error {
	var message = &Message{}
	message.Id = rand.Int63()
	message.Address = address
	message.Data = data
	message.Headers = headers
	message.Options = options
	return o.gateway.Post(message)
}
