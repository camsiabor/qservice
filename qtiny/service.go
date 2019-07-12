package qtiny

import (
	"sync"
	"sync/atomic"
)

type ServiceHandler func(message *Message)

type ServiceOptions map[string]interface{}

type Service struct {
	Address string
	Options ServiceOptions
	Handler ServiceHandler
	Prev    *Service
	Next    *Service
	Mutex   sync.RWMutex

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

func (o *Service) GetConsumer(address string) interface{} {
	o.Mutex.RLock()
	defer o.Mutex.RUnlock()
	if o.consumerMap == nil {
		return nil
	}
	return o.consumerMap[address]
}

func (o *Service) Tail() (result *Service) {
	var current = o
	for current.Next != nil {
		current = current.Next
	}
	return current
}

func (o *Service) Handle(message *Message) {
	var current = o
	for {
		go current.Handler(message)
		if current.Next == nil {
			return
		}
		current = current.Next
	}
}
