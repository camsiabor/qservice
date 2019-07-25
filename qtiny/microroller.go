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

type gatewayLinger struct {
	name    string
	gateway Gateway

	queue   chan *Message
	control chan string

	serial uint64

	requests []*Message
}

type Microroller struct {
	mutex sync.RWMutex

	discovery Discovery

	gatewaysMutex sync.RWMutex

	gateways   map[string]*gatewayLinger
	gatewaydef *gatewayLinger

	nanoMutex sync.RWMutex
	nanos     map[string]*Nano

	requestsLimit uint64

	logger *log.Logger

	ErrHandler OverseerErrorHandler
}

func (o *Microroller) Start(config map[string]interface{}) error {

	o.mutex.Lock()
	defer o.mutex.Unlock()

	var requestsLimit = util.GetUInt64(config, 65536, "requests.limit")

	if o.nanos == nil {
		o.nanos = make(map[string]*Nano)
	}

	var err error

	var pollLimit = util.GetInt(config, 8192, "poll.limit")

	func() {
		o.gatewaysMutex.Lock()
		defer o.gatewaysMutex.Unlock()
		for _, linger := range o.gateways {
			linger.queue, err = linger.gateway.Poll(pollLimit)
			if err != nil {
				break
			}
			linger.serial = 0
			linger.control = make(chan string, 8)
			linger.requests = make([]*Message, requestsLimit)
			for i := 1; i <= runtime.NumCPU(); i++ {
				go o.dispatchLoop(linger)
			}
		}
	}()

	return nil
}

func (o *Microroller) Stop() error {
	o.mutex.Lock()
	defer o.mutex.Unlock()

	o.gatewaysMutex.Lock()
	defer o.gatewaysMutex.Unlock()

	for _, linger := range o.gateways {
		if linger.control != nil {
			close(linger.control)
		}
	}

	return nil
}

func (o *Microroller) dispatchLoop(linger *gatewayLinger) {
	var ok bool
	var msg *Message
	for {
		select {
		case msg, ok = <-linger.queue:
			if !ok {
				break
			}
			o.dispatchMessage(msg, linger)
		case _, ok = <-linger.control:
			if !ok {
				break
			}
		}
	}
}

func (o *Microroller) dispatchMessage(msg *Message, linger *gatewayLinger) {
	defer func() {
		var err = recover()
		if err != nil && o.ErrHandler != nil {
			o.ErrHandler("dispatch", err, o)
		}
	}()

	if msg.Type&MessageTypeReply > 0 {
		o.handleReplyMessage(msg, linger)
		return
	}

	var nano = o.nanos[msg.Address]
	if nano != nil {
		msg.microroller = o
		nano.Handle(msg)
	}
}

func (o *Microroller) handleReplyMessage(response *Message, linger *gatewayLinger) {
	if response.ReplyId < 0 {
		return
	}
	var request = linger.requests[response.ReplyId]

	if request == nil || request.Canceled {
		return
	}

	linger.requests[response.ReplyId] = nil

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

	return o.discovery.NanoLocalRegister(nano)

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

	return o.discovery.NanoLocalUnregister(nano)
}

func (o *Microroller) generateMessageId(linger *gatewayLinger) uint64 {
	var id = atomic.AddUint64(&linger.serial, 1)
	if id >= o.requestsLimit {
		o.mutex.Lock()
		defer o.mutex.Unlock()
		if atomic.LoadUint64(&linger.serial) >= o.requestsLimit {
			atomic.StoreUint64(&linger.serial, 1)
		}
		id = atomic.AddUint64(&linger.serial, 1)
	}
	return id
}

func (o *Microroller) Post(gatekey string, request *Message) (response *Message, err error) {

	var linger = o.getGatewayLinger(gatekey, true)
	if linger == nil {
		return nil, fmt.Errorf("no gateway found with key %v", gatekey)
	}

	if request.Timeout > 0 || request.Handler != nil {

		request.ReplyId = o.generateMessageId(linger)

		linger.requests[request.ReplyId] = request

		if request.Timeout > 0 {
			request.Canceled = false
			if request.ReplyChannel == nil {
				request.ReplyChannel = make(chan *Message)
				defer func() {
					close(request.ReplyChannel)
				}()
			}
		}
	}

	err = linger.gateway.Post(request, o.discovery)
	if err != nil {
		return nil, err
	}

	// synchronous operations
	if request.ReplyChannel != nil {
		var response, timeouted = request.WaitReply(request.Timeout)
		if timeouted {
			request.Canceled = true
			return response, fmt.Errorf("wait for %v reply timeout %d", request.Address, request.Timeout/1000/1000)
		}
	}
	return request.Related, nil
}

func (o *Microroller) Multicast(gatekey string, message *Message) error {
	var linger = o.getGatewayLinger(gatekey, true)
	if linger == nil {
		return fmt.Errorf("no gateway found with key %v", gatekey)
	}
	return linger.gateway.Broadcast(message, o.discovery)
}

func (o *Microroller) Broadcast(gatekey string, message *Message) error {
	var linger = o.getGatewayLinger(gatekey, true)
	if linger == nil {
		return fmt.Errorf("no gateway found with key %v", gatekey)
	}
	return linger.gateway.Broadcast(message, o.discovery)
}

func (o *Microroller) getGatewayLinger(gatekey string, usedef bool) *gatewayLinger {
	if o.gateways == nil {
		return nil
	}
	if len(gatekey) == 0 {
		return o.gatewaydef
	}
	o.gatewaysMutex.RLock()
	var linger = o.gateways[gatekey]
	o.gatewaysMutex.RUnlock()
	if linger == nil && usedef {
		return o.gatewaydef
	}
	return linger
}

func (o *Microroller) SetGateways(gateways map[string]Gateway, defgateway string) *Microroller {

	if len(defgateway) == 0 {
		panic("no default gateway is set")
	}

	if gateways[defgateway] == nil {
		panic("default gateway not found in arguments " + defgateway)
	}

	o.gateways = make(map[string]*gatewayLinger)

	for gatekey, gateway := range gateways {
		var linger = &gatewayLinger{}
		linger.gateway = gateway
		if len(gateway.GetId()) == 0 {
			gateway.SetId(gatekey)
		}
		o.gateways[gatekey] = linger
		if gatekey == defgateway {
			o.gatewaydef = linger
		}
	}

	return o
}

func (o *Microroller) GetDiscovery() Discovery {
	return o.discovery
}

func (o *Microroller) SetDiscovery(discovery Discovery) {
	o.discovery = discovery
}

func (o *Microroller) GetLogger() *log.Logger {
	return o.logger
}

func (o *Microroller) SetLogger(logger *log.Logger) *Microroller {
	o.logger = logger
	return o
}
