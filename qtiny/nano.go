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

	remoteMutex sync.RWMutex
	remoteIndex int32
	remoteCount int
	remoteArray []string
	remoteMap   map[string]interface{}

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

func (o *Nano) RemoteSet(address []string, data []interface{}) {
	o.remoteMutex.Lock()
	defer o.remoteMutex.Unlock()

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
	o.remoteMap = m
	o.remoteArray = address
}

func (o *Nano) RemoteAdd(address string, data interface{}) {
	o.remoteMutex.Lock()
	defer o.remoteMutex.Unlock()

	if o.remoteArray == nil {
		o.remoteArray = []string{address}
	} else {
		for i := 0; i < o.remoteCount; i++ {
			if address == o.remoteArray[i] {
				return
			}
		}
		o.remoteArray = append(o.remoteArray, address)
	}
	if o.remoteMap == nil {
		o.remoteMap = make(map[string]interface{})
	}
	o.remoteMap[address] = data
	o.remoteCount = o.remoteCount + 1
}

func (o *Nano) RemoteRemove(address string) {
	o.remoteMutex.Lock()
	defer o.remoteMutex.Unlock()
	if o.remoteArray == nil {
		return
	}
	var index = -1
	for i := 0; i < o.remoteCount; i++ {
		if o.remoteArray[i] == address {
			index = i
			break
		}
	}
	if index < 0 {
		return
	}
	var novaIndex = 0
	var nova = make([]string, o.remoteCount-1)
	for i := 0; i < o.remoteCount; i++ {
		if i != index {
			nova[novaIndex] = o.remoteArray[i]
			novaIndex = novaIndex + 1
		}
	}
	o.remoteArray = nova

	if o.remoteMap != nil {
		delete(o.remoteMap, address)
	}
	o.remoteCount = o.remoteCount - 1
}

func (o *Nano) RemoteAddresses() []string {
	return o.remoteArray
}

func (o *Nano) RemoteAddress(index int32) string {
	if o.remoteArray == nil {
		return ""
	}
	if index < 0 || int(index) >= o.remoteCount {
		index = o.remoteIndex
		if int(atomic.AddInt32(&o.remoteIndex, 1)) >= o.remoteCount {
			o.remoteIndex = 0
		}
	}
	return o.remoteArray[index]
}

func (o *Nano) RemoteGetData(address string) interface{} {
	o.remoteMutex.RLock()
	defer o.remoteMutex.RUnlock()
	if o.remoteMap == nil {
		return nil
	}
	return o.remoteMap[address]
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
