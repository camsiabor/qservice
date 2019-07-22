package qtiny

import (
	"github.com/camsiabor/qcom/util"
	"github.com/twinj/uuid"
	"math/rand"
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

	locals     []*Nano
	localAlpha *Nano
	localMutex sync.RWMutex

	portalMutex sync.RWMutex
	portalIndex int32
	portalCount int
	portalArray []string
	portals     map[string]interface{}

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

func (o *Nano) Handle(message *Message) {
	if message.Type&MessageTypeBroadcast > 0 {
		for i := 0; i < len(o.locals); i++ {
			o.locals[i].Handler(message)
		}
		return
	}
	var sibling = o.LocalGet(-1)
	sibling.Handler(message)
}

/* ====================================== siblings ==================================== */

func (o *Nano) LocalAdd(silbing *Nano) {
	o.localMutex.Lock()
	defer o.localMutex.Unlock()
	if o.locals == nil {
		o.locals = []*Nano{o, silbing}
	} else {
		o.locals = append(o.locals, silbing)
	}
	silbing.localAlpha = o
}

func (o *Nano) LocalRemove(id string) error {
	if o.locals == nil {
		return nil
	}
	o.localMutex.Lock()
	defer o.localMutex.Unlock()
	var index = -1
	for i := range o.locals {
		if o.locals[i].Id == id {
			index = i
		}
	}

	if index < 0 {
		return nil
	}

	if len(o.locals) == 1 {
		o.locals = nil
		return nil
	}

	var c = 0
	var localsNew = make([]*Nano, len(o.locals)-1)
	for i := range o.locals {
		if i != index {
			localsNew[c] = o.locals[i]
			c++
		}
	}
	o.locals = localsNew
	return nil
}

func (o *Nano) LocalGet(index int) *Nano {
	if o.locals == nil {
		return o
	}
	if index < 0 {
		index = rand.Intn(len(o.locals))
	}
	return o.locals[index]
}

func (o *Nano) LocalAll() []*Nano {
	return o.locals
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
	if index < 0 || int(index) >= len(o.portalArray) {
		index = atomic.AddInt32(&o.portalIndex, 1)
		if int(index) >= len(o.portalArray) {
			index = 0
			o.portalIndex = 0
		}
	}
	return o.portalArray[index]
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
