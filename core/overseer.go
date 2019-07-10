package core

import (
	"fmt"
	"github.com/camsiabor/qcom/util"
	"github.com/twinj/uuid"
	"sync"
	"sync/atomic"
	"time"
)

type OverseerErrorHandler func(event string, err interface{}, overseer *Overseer)

type Overseer struct {
	id string

	serial uint64

	mutex sync.RWMutex

	gateway Gateway

	queue   chan *Message
	control chan string

	requests      []*Message
	requestsLimit uint64

	services map[string]*Service

	ErrHandler OverseerErrorHandler
}

func (o *Overseer) Init(gateway Gateway, waitingLimit uint32) {

	if waitingLimit <= 65536 {
		waitingLimit = 65536
	}

	o.gateway = gateway
	o.requests = make([]*Message, waitingLimit)
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
	err = o.ServiceRegister(o.id, nil, o.handleReply)
	if err != nil {
		return err
	}

	o.queue, err = o.gateway.Poll(8192)
	if err != nil {
		return err
	}
	o.serial = 0
	o.control = make(chan string, 8)
	go o.loop()
	return nil
}

func (o *Overseer) Stop() error {
	if o.control != nil {
		close(o.control)
		o.control = nil
	}
	return nil
}

func (o *Overseer) loop() {
	var ok bool
	var msg *Message

	for {
		select {
		case msg, ok = <-o.queue:
			if !ok {
				break
			}
			o.dispatch(msg)
		case _, ok = <-o.control:
			if !ok {
				break
			}
		}
	}
}

func (o *Overseer) dispatch(msg *Message) {
	defer func() {
		var err = recover()
		if err != nil && o.ErrHandler != nil {
			o.ErrHandler("dispatch", err, o)
		}
	}()
	var service = o.services[msg.Address]
	if service != nil {
		msg.overseer = o
		service.handle(msg)
	}
}

func (o *Overseer) handleReply(response *Message) {
	if response.ReplyId < 0 {
		return
	}
	var request = o.requests[response.ReplyId]
	if request == nil {
		return
	}
	o.requests[response.ReplyId] = nil

	if request.Handler == nil {
		return
	}
	response.Related = request
	request.Handler(response)
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

func (o *Overseer) generateMessageId() uint64 {
	var id = atomic.AddUint64(&o.serial, 1)
	if id >= o.requestsLimit {
		o.mutex.Lock()
		defer o.mutex.Unlock()
		if atomic.LoadUint64(&o.serial) >= o.requestsLimit {
			atomic.StoreUint64(&o.serial, 0)
		}
		id = atomic.AddUint64(&o.serial, 1)
	}
	return id
}

func (o *Overseer) Post(message *Message) (interface{}, error) {

	message.Type = SEND
	message.Sender = o.id

	if message.Timeout > 0 || message.Handler != nil {

		message.ReplyId = o.generateMessageId()
		o.requests[message.ReplyId] = message

		if message.Timeout > 0 {
			defer func() {
				o.requests[message.ReplyId] = nil
			}()
			if message.ReplyChannel == nil {
				message.ReplyChannel = make(chan interface{})
				defer func() {
					close(message.ReplyChannel)
				}()
			}
		}
	}

	var err = o.gateway.Post(message)
	if err != nil {
		return nil, err
	}

	// synchronous operations
	if message.ReplyChannel != nil {
		var ret, isClosed, isTimeout = util.Timeout(message.ReplyChannel, time.Duration(message.Timeout)*time.Millisecond)
		if isClosed {
			return nil, fmt.Errorf("reply channel is closed")
		}
		if isTimeout {
			return nil, fmt.Errorf("reply channel is closed")
		}
		message.ReplyData = ret
		if message.Handler != nil {
			message.Handler(message)
		}
	}

	return message.ReplyData, nil
}

func (o *Overseer) Broadcast(message *Message) error {
	// TODO implement

	message.Type = BROADCAST
	return o.gateway.Post(message)

}
