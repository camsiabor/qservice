package qtiny

import (
	"fmt"
	"github.com/camsiabor/qcom/util"
	"github.com/twinj/uuid"
	"log"
	"runtime"
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

	nanoMutex sync.RWMutex
	nanos     map[string]*Nano

	logger *log.Logger

	ErrHandler OverseerErrorHandler
}

func (o *Microroller) Start(config map[string]interface{}) error {

	o.mutex.Lock()
	defer o.mutex.Unlock()

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
	for i := 1; i <= runtime.NumCPU(); i++ {
		go o.loop()
	}
	return nil
}

func (o *Microroller) Stop() error {
	o.mutex.Lock()
	defer o.mutex.Unlock()

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
		request.Handler(response)
		if request.ReplyChannel != nil {
			request.ReplyChannel <- response
		}
		return
	}
	if request.ReplyChannel != nil {
		request.ReplyChannel <- response
	}
}

func (o *Microroller) NanoLocalRegister(nano *Nano) error {

	if nano == nil {
		return fmt.Errorf("nano is nil")
	}

	if len(nano.Address) == 0 {
		return fmt.Errorf("nano address is not set")
	}

	if nano.Handler == nil {
		return fmt.Errorf("nano handler is not set")
	}

	if len(nano.Id) == 0 {
		nano.Id = uuid.NewV4().String()
	}

	func() {
		o.nanoMutex.Lock()
		defer o.nanoMutex.Unlock()
		var group = o.nanos[nano.Address]
		if group == nil {
			o.nanos[nano.Address] = nano
		} else {
			group.LocalAdd(nano)
		}
	}()

	return o.gateway.NanoLocalRegister(nano)

}

func (o *Microroller) NanoLocalUnregister(nano *Nano) error {

	var err = func() error {
		o.nanoMutex.Lock()
		defer o.nanoMutex.Unlock()

		var group = o.nanos[nano.Address]
		if group == nil {
			return nil
		}
		var err = group.LocalRemove(nano.Id)
		if err != nil {
			return err
		}
		var locals = group.LocalAll()
		if locals == nil || len(locals) == 0 {
			delete(o.nanos, nano.Address)
		}
		return nil
	}()

	if err != nil {
		return err
	}

	return o.gateway.NanoLocalUnregister(nano)
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

func (o *Microroller) SetGateway(gateway Gateway) *Microroller {
	o.gateway = gateway
	return o
}

func (o *Microroller) GetLogger() *log.Logger {
	return o.logger
}

func (o *Microroller) SetLogger(logger *log.Logger) *Microroller {
	o.logger = logger
	return o
}
