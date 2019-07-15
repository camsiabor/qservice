package qtiny

import (
	"fmt"
	"github.com/camsiabor/qcom/util"
	"sync"
	"sync/atomic"
)

type OverseerErrorHandler func(event string, err interface{}, microroller *Microroller)

type Microroller struct {
	serial uint64

	mutex sync.RWMutex

	gateway Gateway

	queue   chan *Message
	control chan string

	requests      []*Message
	requestsLimit uint64

	nanos map[string]*Nano

	ErrHandler OverseerErrorHandler
}

func (o *Microroller) Init(gateway Gateway, waitingLimit uint32) {

}

func (o *Microroller) Start(config map[string]interface{}) error {

	var requestsLimit = util.GetUInt64(config, 65536, "requests.limit")
	o.requests = make([]*Message, requestsLimit)

	if o.nanos == nil {
		o.nanos = make(map[string]*Nano)
	}

	if o.gateway == nil {
		o.gateway = util.Get(config, nil, "gateway").(Gateway)
	}

	if o.gateway == nil {
		return fmt.Errorf("gateway is null")
	}

	var err error

	var pollLimit = util.GetInt(config, 8192, "poll.limit")
	o.queue, err = o.gateway.Poll(pollLimit)
	if err != nil {
		return err
	}
	o.serial = 0
	o.control = make(chan string, 8)
	go o.loop()
	return nil
}

func (o *Microroller) Stop() error {
	if o.control != nil {
		close(o.control)
		o.control = nil
	}
	return nil
}

func (o *Microroller) loop() {
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

func (o *Microroller) dispatch(msg *Message) {
	defer func() {
		var err = recover()
		if err != nil && o.ErrHandler != nil {
			o.ErrHandler("dispatch", err, o)
		}
	}()

	if msg.Type&MessageTypeReply > 0 {
		o.handleReply(msg)
		return
	}

	var nano = o.nanos[msg.Address]
	if nano != nil {
		msg.microroller = o
		nano.Handle(msg)
	}
}

func (o *Microroller) handleReply(response *Message) {
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

func (o *Microroller) NanoLocalRegister(address string, flag NanoFlag, options NanoOptions, handler NanoHandler) error {

	o.mutex.Lock()
	defer o.mutex.Unlock()

	var nano = &Nano{}
	nano.Address = address
	nano.Handler = handler

	var current = o.nanos[address]
	if current == nil {
		o.nanos[address] = nano
	} else {
		current.LocalAdd(nano)
	}

	return o.gateway.NanoLocalRegister(address, flag, options)

}

func (o *Microroller) NanoLocalUnregister(address string) error {

	o.mutex.Lock()
	defer o.mutex.Unlock()

	delete(o.nanos, address)

	return o.gateway.NanoLocalUnregister(address)
}

func (o *Microroller) generateMessageId() uint64 {
	var id = atomic.AddUint64(&o.serial, 1)
	if id >= o.requestsLimit {
		o.mutex.Lock()
		defer o.mutex.Unlock()
		if atomic.LoadUint64(&o.serial) >= o.requestsLimit {
			atomic.StoreUint64(&o.serial, 1)
		}
		id = atomic.AddUint64(&o.serial, 1)
	}
	return id
}

func (o *Microroller) Post(request *Message) (response *Message, err error) {

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

	err = o.gateway.Post(request)
	if err != nil {
		return nil, err
	}

	// synchronous operations
	if request.ReplyChannel != nil {
		var response, timeouted = request.WaitReply(request.Timeout)
		if timeouted {
			return response, fmt.Errorf("wait for %v reply timeout %d", request.Address, request.Timeout/1000/1000)
		}
	}
	return request.Related, nil
}

func (o *Microroller) Broadcast(message *Message) error {
	// TODO implement

	message.Type = MessageTypeBroadcast
	return o.gateway.Post(message)

}

func (o *Microroller) GetGateway() Gateway {
	return o.gateway
}

func (o *Microroller) SetGateway(gateway Gateway) {
	o.gateway = gateway
}
