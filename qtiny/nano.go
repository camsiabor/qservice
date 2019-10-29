package qtiny

import (
	"fmt"
	"github.com/camsiabor/qcom/util"
	"github.com/twinj/uuid"
	"sync"
	"sync/atomic"
)

type NanoFlag int
type NanoEvent int
type NanoOptions map[string]interface{}
type NanoHandler func(message *Message)
type NanoCallback func(event NanoEvent, nano *Nano, context interface{})

const (
	NanoEventStop = 0x0010
)

const (
	NanoFlagLocalOnly NanoFlag = 1
)

type Nano struct {
	Id string

	Address string
	Options NanoOptions
	Handler NanoHandler

	Flag NanoFlag

	localPrev  *Nano
	localNext  *Nano
	localMutex sync.RWMutex

	portalMutex   sync.RWMutex
	portalPointer int32
	portalCount   int
	portalArray   []string
	portals       map[string]interface{}

	shareQueue chan *Message
	selfQueue  chan *Message

	callbacks []NanoCallback
}

func NewNano(address string, flag NanoFlag, options NanoOptions, handler NanoHandler) *Nano {
	var nano = &Nano{}
	nano.Id = uuid.NewV4().String()
	nano.Address = address
	nano.Flag = flag
	nano.Options = options
	nano.Handler = handler
	return nano
}

func CloneNano(nano *Nano) *Nano {
	if nano == nil {
		panic("invalid argument nil nano")
	}
	return &Nano{
		Id:        nano.Id,
		Flag:      nano.Flag,
		Address:   nano.Address,
		Options:   nano.Options,
		Handler:   nano.Handler,
		callbacks: nano.callbacks,
	}
}

/* ====================================== handling ==================================== */

func (o *Nano) Dispatch(message *Message) {

	// broadcast message
	if message.Type&MessageTypeBroadcast > 0 {
		if o.localNext != nil {
			var current = o
			for current != nil {
				current.selfQueue <- message
				current = current.localNext
			}
		}
		return
	}

	// one message
	o.shareQueue <- message
}

func (o *Nano) handleLoop() {
	var ok = true
	var message *Message
	for ok {
		select {
		case message, ok = <-o.shareQueue:
			o.handle(message)
		case message, ok = <-o.selfQueue:
			o.handle(message)
		}
	}
}

func (o *Nano) handle(message *Message) {
	if message == nil {
		return
	}
	defer func() {
		var pan = recover()
		if pan != nil {
			var err = util.AsError(pan)
			_ = message.Error(500, fmt.Sprintf("%v | error | %v | ", o.Address, err))
		}
	}()
	o.Handler(message)
}

/* ====================================== siblings ==================================== */

func (o *Nano) LocalAdd(silbing *Nano) {

	if silbing.Handler == nil {
		panic("sibling no handler")
	}

	o.localMutex.Lock()
	defer o.localMutex.Unlock()

	if o == silbing {
		if o.shareQueue == nil {
			o.shareQueue = make(chan *Message, 1024)
		}
	}

	if silbing.selfQueue == nil {
		silbing.selfQueue = make(chan *Message, 128)
	}

	if o != silbing {
		silbing.localNext = o.localNext
		silbing.localPrev = o
		silbing.shareQueue = o.shareQueue

		o.localNext = silbing
	}

	go o.handleLoop()
}

func (o *Nano) LocalRemove(id string) error {

	o.localMutex.Lock()
	defer o.localMutex.Unlock()

	if o.Id == id {
		if o.selfQueue != nil {
			close(o.selfQueue)
		}
		if o.localNext == nil {
			if o.shareQueue != nil {
				close(o.shareQueue)
			}
		}
	}

	var current = o.localNext
	for current != nil {
		if current.Id == id {
			if current.localPrev != nil {
				current.localPrev.localNext = current.localNext
			}
			if current.selfQueue != nil {
				close(current.selfQueue)
			}
		}
		current = current.localNext
	}
	return nil
}

func (o *Nano) LocalIterate(routine func(*Nano) (bool, error)) error {
	var current = o.localNext
	for current != nil {
		var run, err = routine(current)
		if !run {
			return nil
		}
		if err != nil {
			return err
		}
		current = current.localNext
		if current == o {
			return nil
		}
	}
	return nil
}

/* ====================================== remote consumers ==================================== */

func (o *Nano) PortalSet(address []string, data []interface{}) {
	o.portalMutex.Lock()
	defer o.portalMutex.Unlock()

	var m = make(map[string]interface{})
	for i := range address {
		var oneaddr = address[i]
		var onedata interface{}
		if data == nil {
			onedata = oneaddr
		} else {
			onedata = data[i]
		}
		if onedata == nil {
			onedata = oneaddr
		}
		m[oneaddr] = onedata
	}
	o.portals = m
	o.portalArray = address
	o.portalCount = len(o.portalArray)
}

func (o *Nano) PortalAdd(address string, data interface{}) {
	o.portalMutex.Lock()
	defer o.portalMutex.Unlock()

	if o.portalArray == nil {
		o.portalArray = []string{address}
	} else {
		for i := 0; i < o.portalCount; i++ {
			if address == o.portalArray[i] {
				return
			}
		}
		o.portalArray = append(o.portalArray, address)
	}
	if o.portals == nil {
		o.portals = make(map[string]interface{})
	}
	o.portals[address] = data
	o.portalCount = len(o.portalArray)
}

func (o *Nano) PortalRemove(address string) {
	o.portalMutex.Lock()
	defer o.portalMutex.Unlock()
	if o.portalArray == nil {
		return
	}
	var index = -1
	for i := 0; i < o.portalCount; i++ {
		if o.portalArray[i] == address {
			index = i
			break
		}
	}
	if index < 0 {
		return
	}
	var novaIndex = 0
	var nova = make([]string, o.portalCount-1)
	for i := 0; i < o.portalCount; i++ {
		if i != index {
			nova[novaIndex] = o.portalArray[i]
			novaIndex = novaIndex + 1
		}
	}
	o.portalArray = nova

	if o.portals != nil {
		delete(o.portals, address)
	}
	o.portalCount = len(o.portalArray)
}

func (o *Nano) PortalAddresses() []string {
	return o.portalArray
}

func (o *Nano) PortalAddress(index int32) string {
	if o.portalArray == nil {
		return ""
	}
	var array = o.portalArray
	var n = len(array)
	if index < 0 || int(index) >= n {
		index = atomic.AddInt32(&o.portalPointer, 1)
		if int(index) >= n {
			index = 0
			o.portalPointer = 0
		}
	}
	return array[index]
}

func (o *Nano) PortalPointer() ([]string, int) {
	var array = o.portalArray
	if array == nil {
		return nil, 0
	}
	var n = len(array)
	if n == 1 {
		return o.portalArray, 0
	}
	var pointer = int(atomic.AddInt32(&o.portalPointer, 1))
	if pointer >= n {
		pointer = 0
		o.portalPointer = 0
	}
	return array, pointer
}

func (o *Nano) PortalCount() int {
	return o.portalCount
}

func (o *Nano) GatewayGetData(address string) interface{} {
	o.portalMutex.RLock()
	defer o.portalMutex.RUnlock()
	if o.portals == nil {
		return nil
	}
	return o.portals[address]
}

/* ======================= callback ====================== */

func (o *Nano) CallbackAdd(callback NanoCallback) *Nano {
	if callback == nil {
		return o
	}
	o.localMutex.Lock()
	defer o.localMutex.Unlock()
	if o.callbacks == nil {
		o.callbacks = []NanoCallback{callback}
	} else {
		o.callbacks = append(o.callbacks, callback)
	}
	return o
}

func (o *Nano) CallbackInvoke(event NanoEvent, context interface{}, dopanic bool) (err error) {
	if o.callbacks == nil {
		return nil
	}
	for _, callback := range o.callbacks {
		func() {
			defer func() {
				var pan = recover()
				if pan != nil {
					if dopanic {
						panic(pan)
					} else {
						err = util.AsError(pan)
					}
				}
			}()
			callback(event, o, context)
		}()
	}
	return err
}
