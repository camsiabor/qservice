package memory

import (
	"fmt"
	"github.com/camsiabor/qcom/util"
	"github.com/camsiabor/qservice/qtiny"
	"github.com/twinj/uuid"
	"log"
	"sync"
)

type MemDiscovery struct {
	id  string
	tag string

	Mutex sync.Mutex

	Looping bool
	Logger  *log.Logger

	Listeners []chan *qtiny.Message

	Discovery qtiny.Discovery

	RemotesMutex sync.RWMutex
	Remotes      map[string]*qtiny.Nano

	LocalsMutex sync.RWMutex
	Locals      map[string]*qtiny.Nano
}

func (o *MemDiscovery) Start(config map[string]interface{}) error {

	var configId = util.GetStr(config, "", "id")
	if len(configId) > 0 {
		o.id = configId
	}

	if len(o.id) == 0 {
		o.id = uuid.NewV4().String()
	}

	if o.Remotes == nil {
		o.Remotes = make(map[string]*qtiny.Nano)
	}

	if o.Looping {
		return fmt.Errorf("already running")
	}

	o.Looping = true
	return nil
}

func (o *MemDiscovery) Stop(map[string]interface{}) error {
	o.Looping = false
	return nil
}

func (o *MemDiscovery) NanoRemoteRegister(nano *qtiny.Nano) error {
	o.RemotesMutex.Lock()
	defer o.RemotesMutex.Unlock()
	if o.Remotes == nil {
		o.Remotes = make(map[string]*qtiny.Nano)
	}
	var service = o.Remotes[nano.Address]
	if service == nil {
		service.Address = nano.Address
		o.Remotes[nano.Address] = nano
	}
	return nil
}

func (o *MemDiscovery) NanoRemoteUnregister(nano *qtiny.Nano) error {
	if o.Remotes == nil || nano == nil {
		return nil
	}
	o.RemotesMutex.Lock()
	defer o.RemotesMutex.Unlock()
	delete(o.Remotes, nano.Address)
	return nil
}

func (o *MemDiscovery) NanoRemoteGet(address string) (*qtiny.Nano, error) {
	o.RemotesMutex.RLock()
	defer o.RemotesMutex.RUnlock()
	if o.Remotes == nil {
		return nil, nil
	}
	return o.Remotes[address], nil
}

func (o *MemDiscovery) NanoLocalRegister(nano *qtiny.Nano) error {
	o.LocalsMutex.Lock()
	defer o.LocalsMutex.Unlock()
	if o.Locals == nil {
		o.Locals = make(map[string]*qtiny.Nano)
	}
	var service = o.Locals[nano.Address]
	if service == nil {
		service.Address = nano.Address
		o.Locals[nano.Address] = nano
	}
	return nil
}

func (o *MemDiscovery) NanoLocalUnregister(nano *qtiny.Nano) error {
	if o.Locals == nil || nano == nil {
		return nil
	}
	o.LocalsMutex.Lock()
	defer o.LocalsMutex.Unlock()
	delete(o.Locals, nano.Address)
	return nil
}

func (o *MemDiscovery) NanoLocalGet(address string) (*qtiny.Nano, error) {
	o.LocalsMutex.RLock()
	defer o.LocalsMutex.RUnlock()
	if o.Locals == nil {
		return nil, nil
	}
	return o.Locals[address], nil
}

/* ====================================== subscribers ===================================== */

func (o *MemDiscovery) NanoLocalAdd(nano *qtiny.Nano) {

	o.LocalsMutex.Lock()
	defer o.LocalsMutex.Unlock()
	if o.Locals == nil {
		o.Locals = make(map[string]*qtiny.Nano)
	}

	var local = o.Locals[nano.Address]
	if local == nil {
		local = &qtiny.Nano{}
		local.Address = nano.Address
		local.Options = nano.Options
		o.Locals[nano.Address] = local
	}
}

func (o *MemDiscovery) NanoLocalRemove(address string) {
	if o.Locals == nil {
		return
	}
	o.LocalsMutex.Lock()
	defer o.LocalsMutex.Unlock()
	delete(o.Locals, address)
}

func (o *MemDiscovery) NanoLocalAll() map[string]*qtiny.Nano {
	if o.Locals == nil {
		return nil
	}
	var m = map[string]*qtiny.Nano{}
	o.LocalsMutex.RLock()
	defer o.LocalsMutex.RUnlock()
	for k, v := range o.Locals {
		m[k] = v
	}
	return m
}

/* ============================================================================================= */

func (o *MemDiscovery) GetId() string {
	return o.id
}

func (o *MemDiscovery) GetTag() string {
	return o.tag
}

func (o *MemDiscovery) GetLogger() *log.Logger {
	return o.Logger
}

func (o *MemDiscovery) SetLogger(logger *log.Logger) {
	o.Logger = logger
}

func (o *MemDiscovery) GetDiscovery() qtiny.Discovery {
	return o.Discovery
}

func (o *MemDiscovery) SetDiscovery(discovery qtiny.Discovery) {
	o.Discovery = discovery
}
