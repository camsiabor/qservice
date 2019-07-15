package qtiny

import (
	"math/rand"
	"sync"
	"sync/atomic"
)

type NanoHandler func(message *Message)

type NanoOptions map[string]interface{}

type NanoFlag int

const (
	NanoFlagLocalOnly NanoFlag = 1
)

type Nano struct {
	TinyGroup string

	Address string
	Options NanoOptions
	Handler NanoHandler

	Flag NanoFlag

	Locals     []*Nano
	LocalAlpha *Nano
	LocalMutex sync.RWMutex

	RemoteMutex sync.RWMutex
	RemoteIndex int32
	RemoteCount int
	RemoteArray []string
	RemoteMap   map[string]interface{}
}

/* ====================================== handling ==================================== */

func (o *Nano) Handle(message *Message) {
	if message.Type&MessageTypeBroadcast > 0 {
		for i := 0; i < len(o.Locals); i++ {
			o.Locals[i].Handler(message)
		}
		return
	}
	var sibling = o.LocalGet(-1)
	sibling.Handler(message)
}

/* ====================================== siblings ==================================== */

func (o *Nano) LocalAdd(silbing *Nano) {
	o.LocalMutex.Lock()
	defer o.LocalMutex.Unlock()
	if o.Locals == nil {
		o.Locals = []*Nano{o, silbing}
	} else {
		o.Locals = append(o.Locals, silbing)
	}
	silbing.LocalAlpha = o
}

func (o *Nano) LocalGet(index int) *Nano {
	if o.Locals == nil {
		return o
	}
	if index < 0 {
		index = rand.Intn(len(o.Locals))
	}
	return o.Locals[index]
}

/* ====================================== remote consumers ==================================== */

func (o *Nano) RemoteSet(address []string, data []interface{}) {
	o.RemoteMutex.Lock()
	defer o.RemoteMutex.Unlock()

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
	o.RemoteMap = m
	o.RemoteArray = address
}

func (o *Nano) RemoteAdd(address string, data interface{}) {
	o.RemoteMutex.Lock()
	defer o.RemoteMutex.Unlock()

	if o.RemoteArray == nil {
		o.RemoteArray = []string{address}
	} else {
		for i := 0; i < o.RemoteCount; i++ {
			if address == o.RemoteArray[i] {
				return
			}
		}
		o.RemoteArray = append(o.RemoteArray, address)
	}
	if o.RemoteMap == nil {
		o.RemoteMap = make(map[string]interface{})
	}
	o.RemoteMap[address] = data
	o.RemoteCount = o.RemoteCount + 1
}

func (o *Nano) RemoteRemove(address string) {
	o.RemoteMutex.Lock()
	defer o.RemoteMutex.Unlock()
	if o.RemoteArray == nil {
		return
	}
	var index = -1
	for i := 0; i < o.RemoteCount; i++ {
		if o.RemoteArray[i] == address {
			index = i
			break
		}
	}
	if index < 0 {
		return
	}
	var novaIndex = 0
	var nova = make([]string, o.RemoteCount-1)
	for i := 0; i < o.RemoteCount; i++ {
		if i != index {
			nova[novaIndex] = o.RemoteArray[i]
			novaIndex = novaIndex + 1
		}
	}
	o.RemoteArray = nova

	if o.RemoteMap != nil {
		delete(o.RemoteMap, address)
	}
	o.RemoteCount = o.RemoteCount - 1
}

func (o *Nano) RemoteAddresses() []string {
	return o.RemoteArray
}

func (o *Nano) RemoteAddress(index int32) string {
	if o.RemoteArray == nil {
		return ""
	}
	if index < 0 || int(index) >= o.RemoteCount {
		index = o.RemoteIndex
		if int(atomic.AddInt32(&o.RemoteIndex, 1)) >= o.RemoteCount {
			o.RemoteIndex = 0
		}
	}
	return o.RemoteArray[index]
}

func (o *Nano) RemoteGetData(address string) interface{} {
	o.RemoteMutex.RLock()
	defer o.RemoteMutex.RUnlock()
	if o.RemoteMap == nil {
		return nil
	}
	return o.RemoteMap[address]
}
