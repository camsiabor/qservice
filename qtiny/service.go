package qtiny

import (
	"math/rand"
	"sync"
	"sync/atomic"
)

type ServiceHandler func(message *Message)

type ServiceOptions map[string]interface{}

type Service struct {
	Address string
	Options ServiceOptions
	Handler ServiceHandler

	Siblings   []*Service
	BigBrother *Service

	Mutex sync.RWMutex

	consumerPoint int32
	consumerCount int
	consumerArray []string
	consumerMap   map[string]interface{}
}

func (o *Service) ConsumerAdd(address string, data interface{}) {
	o.Mutex.Lock()
	defer o.Mutex.Unlock()

	if o.consumerArray == nil {
		o.consumerArray = []string{address}
	} else {
		for i := 0; i < o.consumerCount; i++ {
			if address == o.consumerArray[i] {
				return
			}
		}
		o.consumerArray = append(o.consumerArray, address)
	}
	if o.consumerMap == nil {
		o.consumerMap = make(map[string]interface{})
	}
	o.consumerMap[address] = data
	o.consumerCount = o.consumerCount + 1
}

func (o *Service) ConsumerRemove(address string) {
	o.Mutex.Lock()
	defer o.Mutex.Unlock()
	if o.consumerArray == nil {
		return
	}
	var index = -1
	for i := 0; i < o.consumerCount; i++ {
		if o.consumerArray[i] == address {
			index = i
			break
		}
	}
	if index < 0 {
		return
	}
	var novaIndex = 0
	var nova = make([]string, o.consumerCount-1)
	for i := 0; i < o.consumerCount; i++ {
		if i != index {
			nova[novaIndex] = o.consumerArray[i]
			novaIndex = novaIndex + 1
		}
	}
	o.consumerArray = nova

	if o.consumerMap != nil {
		delete(o.consumerMap, address)
	}
	o.consumerCount = o.consumerCount - 1
}

func (o *Service) GetConsumerAddresses() []string {
	return o.consumerArray
}

func (o *Service) GetConsumerAddress(index int32) string {
	if o.consumerArray == nil {
		return ""
	}
	if index < 0 {
		index = o.consumerPoint
		if int(atomic.AddInt32(&o.consumerPoint, 1)) > o.consumerCount {
			o.consumerPoint = 0
		}
	}
	if int(index) >= o.consumerCount {
		index = 0
	}
	return o.consumerArray[index]
}

func (o *Service) GetConsumerData(address string) interface{} {
	o.Mutex.RLock()
	defer o.Mutex.RUnlock()
	if o.consumerMap == nil {
		return nil
	}
	return o.consumerMap[address]
}

func (o *Service) AddSibling(silbing *Service) {
	o.Mutex.Lock()
	defer o.Mutex.Unlock()
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
