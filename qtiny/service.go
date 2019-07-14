package qtiny

import (
	"math/rand"
	"sync"
	"sync/atomic"
)

type ServiceHandler func(message *Message)

type ServiceOptions map[string]interface{}

type Service struct {
	Id      uint64
	Address string
	Options ServiceOptions
	Handler ServiceHandler

	Siblings   []*Service
	BigBrother *Service

	mutex sync.RWMutex

	nodeMutex sync.RWMutex
	nodePoint int32
	nodeCount int
	nodeArray []string
	nodeMap   map[string]interface{}
}

/* ====================================== handling ==================================== */

func (o *Service) Handle(message *Message) {
	if message.Type&MessageTypeBroadcast > 0 {
		for i := 0; i < len(o.Siblings); i++ {
			o.Siblings[i].Handler(message)
		}
		return
	}
	var sibling = o.GetSibling(-1)
	sibling.Handler(message)
}

/* ====================================== siblings ==================================== */

func (o *Service) AddSibling(silbing *Service) {
	o.mutex.Lock()
	defer o.mutex.Unlock()
	if o.Siblings == nil {
		o.Siblings = []*Service{o, silbing}
	} else {
		o.Siblings = append(o.Siblings, silbing)
	}
	silbing.BigBrother = o
}

func (o *Service) GetSibling(index int) *Service {
	if o.Siblings == nil {
		return o
	}
	if index < 0 {
		index = rand.Intn(len(o.Siblings))
	}
	return o.Siblings[index]
}

/* ====================================== remote consumers ==================================== */

func (o *Service) NodeSet(address []string, data []interface{}) {
	o.nodeMutex.Lock()
	defer o.nodeMutex.Unlock()

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
	o.nodeMap = m
	o.nodeArray = address
}

func (o *Service) NodeAdd(address string, data interface{}) {
	o.nodeMutex.Lock()
	defer o.nodeMutex.Unlock()

	if o.nodeArray == nil {
		o.nodeArray = []string{address}
	} else {
		for i := 0; i < o.nodeCount; i++ {
			if address == o.nodeArray[i] {
				return
			}
		}
		o.nodeArray = append(o.nodeArray, address)
	}
	if o.nodeMap == nil {
		o.nodeMap = make(map[string]interface{})
	}
	o.nodeMap[address] = data
	o.nodeCount = o.nodeCount + 1
}

func (o *Service) NodeRemove(address string) {
	o.nodeMutex.Lock()
	defer o.nodeMutex.Unlock()
	if o.nodeArray == nil {
		return
	}
	var index = -1
	for i := 0; i < o.nodeCount; i++ {
		if o.nodeArray[i] == address {
			index = i
			break
		}
	}
	if index < 0 {
		return
	}
	var novaIndex = 0
	var nova = make([]string, o.nodeCount-1)
	for i := 0; i < o.nodeCount; i++ {
		if i != index {
			nova[novaIndex] = o.nodeArray[i]
			novaIndex = novaIndex + 1
		}
	}
	o.nodeArray = nova

	if o.nodeMap != nil {
		delete(o.nodeMap, address)
	}
	o.nodeCount = o.nodeCount - 1
}

func (o *Service) NodeAddresses() []string {
	return o.nodeArray
}

func (o *Service) NodeAddress(index int32) string {
	if o.nodeArray == nil {
		return ""
	}
	if index < 0 || int(index) >= o.nodeCount {
		index = o.nodePoint
		if int(atomic.AddInt32(&o.nodePoint, 1)) >= o.nodeCount {
			o.nodePoint = 0
		}
	}
	return o.nodeArray[index]
}

func (o *Service) NodeGetData(address string) interface{} {
	o.nodeMutex.RLock()
	defer o.nodeMutex.RUnlock()
	if o.nodeMap == nil {
		return nil
	}
	return o.nodeMap[address]
}
