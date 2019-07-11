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
	LifeCycler

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

}

func (o *Overseer) Start(config map[string]interface{}) error {

	var requestsLimit = util.GetUInt64(config, 65536, "requests.limit")
	o.requests = make([]*Message, requestsLimit)

	if o.services == nil {
		o.services = make(map[string]*Service)
	}

	if o.gateway == nil {
		o.gateway = util.Get(config, nil, "gateway").(Gateway)
	}

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

	request.Related = response
	response.Related = request

	if request.Handler != nil {
		go func() {
			request.Handler(response)
			if request.ReplyChannel != nil {
				request.ReplyChannel <- response
			}
		}()
		return
	}

	if request.ReplyChannel != nil {
		request.ReplyChannel <- response
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

func (o *Overseer) Post(request *Message) (*Message, error) {

	request.Type = SEND
	request.Sender = o.id

	if request.Timeout > 0 || request.Handler != nil {

		request.ReplyId = o.generateMessageId()
		o.requests[request.ReplyId] = request

		if request.Timeout > 0 {
			defer func() {
				o.requests[request.ReplyId] = nil
			}()
			if request.ReplyChannel == nil {
				request.ReplyChannel = make(chan *Message)
				defer func() {
					close(request.ReplyChannel)
				}()
			}
		}
	}

	var err = o.gateway.Post(request)
	if err != nil {
		return nil, err
	}

	// synchronous operations
	if request.ReplyChannel != nil {
		var response, timeouted = request.WaitReply(time.Duration(request.Timeout) * time.Millisecond)
		if timeouted {
			return response, fmt.Errorf("wait for %v reply timeout %d", request.Address, request.Timeout)
		}
	}
	return request.Related, nil
}

func (o *Overseer) Broadcast(message *Message) error {
	// TODO implement

	message.Type = BROADCAST
	return o.gateway.Post(message)

}
