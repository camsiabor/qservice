package qtiny

import (
	"github.com/camsiabor/qcom/qerr"
	"github.com/camsiabor/qcom/util"
	"github.com/twinj/uuid"
	"log"
	"runtime"
	"sync"
	"sync/atomic"
)

type OverseerErrorHandler func(event string, err interface{}, microroller *Microroller)

type gatewayLinger struct {
	gatekey string
	gateway Gateway

	queue   chan *Message
	control chan string

	serial uint64

	requests []*Message
}

type Microroller struct {
	mutex sync.RWMutex

	tina      *Tina
	discovery Discovery

	gatewaysMutex sync.RWMutex

	gateways   map[string]*gatewayLinger
	gatewaydef *gatewayLinger

	nanoMutex sync.RWMutex
	nanos     map[string]*Nano

	requestsLimit uint64

	logger *log.Logger

	ErrHandler OverseerErrorHandler

	Verbose int
}

func (o *Microroller) GetTina() *Tina {
	return o.tina
}

func (o *Microroller) Start(config map[string]interface{}) error {

	o.mutex.Lock()
	defer o.mutex.Unlock()

	o.Verbose = util.GetInt(config, 0, "verbose")

	var requestsLimit = util.GetUInt64(config, 1024*128, "requests.limit")

	if o.nanos == nil {
		o.nanos = make(map[string]*Nano)
	}

	var err error

	var pollLimit = util.GetInt(config, 1024*8, "poll.limit")

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
		var pan = recover()
		if pan == nil {
			return
		}
		if o.Verbose > 0 {
			o.logger.Printf(qerr.StackString(1, o.Verbose, "[microroller] handle error %v | %v", util.AsError(pan), msg.String()))
		}
		if o.ErrHandler != nil {
			o.ErrHandler("dispatch", pan, o)
		}
	}()

	if msg.Type&MessageTypeReply > 0 {
		if o.Verbose > 0 {
			o.logger.Printf("[microroller] handle reply %v", msg.String())
		}
		o.handleReplyMessage(msg, linger)
		return
	}

	var nano = o.nanos[msg.Address]
	if nano == nil {
		if msg.microroller == nil {
			msg.microroller = o
		}
		_ = msg.Error(404, "["+linger.gatekey+"] no service found "+msg.Address)
	} else {
		if o.Verbose > 0 {
			o.logger.Printf("[microroller] dispatch %v", msg.String())
		}
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
		return qerr.StackStringErr(0, 1024, "nano is nil")
	}

	if len(nano.Address) == 0 {
		return qerr.StackStringErr(0, 1024, "nano address is not set")
	}

	if nano.Handler == nil {
		return qerr.StackStringErr(0, 1024, "nano handler is not set")
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
	if id >= uint64(len(linger.requests)) {
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

	if o.Verbose > 0 {
		o.logger.Printf(qerr.StackString(0, o.Verbose, "[microroller] posting %v", request.String()))
	}
	var linger = o.getGatewayLinger(gatekey, true)
	if linger == nil {
		return nil, qerr.StackStringErr(0, 1024, "no gateway found with key %v", gatekey)
	}
	request.Gatekey = linger.gatekey

	if request.Type != MessageTypeReply && (request.Timeout > 0 || request.Handler != nil) {

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
			if o.Verbose > 0 {
				o.logger.Printf(qerr.StackString(0, o.Verbose, "[microroller] response timeout %v", request.String()))
			}
			return response, qerr.StackStringErr(0, 1024, "wait for %v reply timeout %d", request.Address, request.Timeout/1000/1000)
		}
		if o.Verbose > 0 {
			o.logger.Printf(qerr.StackString(0, o.Verbose, "[microroller] response receveid [request] %v", request.String()))
			o.logger.Printf("[microroller] response receveid [response] %v", response.String())
		}
	}
	return request.Related, nil
}

func (o *Microroller) Multicast(gatekey string, message *Message) error {
	var linger = o.getGatewayLinger(gatekey, true)
	if linger == nil {
		return qerr.StackStringErr(0, 1024, "no gateway found with key %v", gatekey)
	}
	return linger.gateway.Broadcast(message, o.discovery)
}

func (o *Microroller) Broadcast(gatekey string, message *Message) error {
	var linger = o.getGatewayLinger(gatekey, true)
	if linger == nil {
		return qerr.StackStringErr(0, 1024, "no gateway found with key %v", gatekey)
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

func (o *Microroller) SetGateways(gateways map[string]Gateway, gatewaydef string) *Microroller {

	if len(gatewaydef) > 0 && gateways[gatewaydef] == nil {
		panic("default gateway not found in arguments " + gatewaydef)
	}

	o.gateways = make(map[string]*gatewayLinger)

	for gatekey, gateway := range gateways {
		var linger = &gatewayLinger{}
		linger.gatekey = gatekey
		linger.gateway = gateway
		if len(gateway.GetId()) == 0 {
			gateway.SetId(gatekey)
		}
		o.gateways[gatekey] = linger
		if len(gatewaydef) == 0 {
			gatewaydef = gatekey
		}
		if gatekey == gatewaydef {
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
