package memory

import (
	"github.com/camsiabor/qcom/qerr"
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

	// local nanos
	LocalsMutex sync.RWMutex
	Locals      map[string]*qtiny.Nano

	// remote nanos
	RemotesMutex sync.RWMutex
	Remotes      map[string]*qtiny.Nano

	// local gateway
	GatewaysMutex sync.RWMutex
	Gateways      map[string]qtiny.Gateway

	// remote portal
	PortalsMutex sync.RWMutex
	Portals      map[string]*qtiny.Portal

	LocalAsRemote bool
}

func (o *MemDiscovery) Start(config map[string]interface{}) error {

	var configId = util.GetStr(config, "", "id")
	if len(configId) > 0 {
		o.id = configId
	}

	if len(o.id) == 0 {
		o.id = uuid.NewV4().String()
	}

	o.tag = uuid.NewV4().String()

	o.LocalAsRemote = util.GetBool(config, false, "remote.as.local")

	if o.Remotes == nil {
		o.Remotes = make(map[string]*qtiny.Nano)
	}

	if o.Portals == nil {
		o.Portals = make(map[string]*qtiny.Portal)
	}

	if o.Looping {
		return qerr.StackStringErr(0, 1024, "already running")
	}

	o.Looping = true

	if o.LocalAsRemote {
		func() {
			o.GatewaysMutex.Lock()
			defer o.GatewaysMutex.Unlock()
			for gatekey, gateway := range o.Gateways {
				var ch, err = gateway.EventChannelGet(gatekey)
				if err == nil {
					go o.gatewayEventLoop(gateway, ch)
				}
			}
		}()
	}

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
		service = qtiny.CloneNano(nano)
		if o.LocalAsRemote {
			service.PortalAdd(o.GetNodeId(), map[string]interface{}{})
		}
		o.Remotes[service.Address] = service
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
	var remote = o.Remotes[address]
	return remote, nil
}

func (o *MemDiscovery) NanoLocalRegister(nano *qtiny.Nano) error {
	o.LocalsMutex.Lock()
	defer o.LocalsMutex.Unlock()
	if o.Locals == nil {
		o.Locals = make(map[string]*qtiny.Nano)
	}
	var service = o.Locals[nano.Address]
	if service == nil {
		service = qtiny.CloneNano(nano)
		o.Locals[service.Address] = service
	}

	if o.LocalAsRemote {
		nano.PortalAdd(o.GetNodeId(), map[string]interface{}{})
		return o.NanoRemoteRegister(nano)
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

	if o.LocalAsRemote {
		return o.NanoRemoteUnregister(nano)
	}

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

/* =============================== gateway ============================================================== */

func (o *MemDiscovery) GatewayPublish(gatekey string, gateway qtiny.Gateway) error {
	o.GatewaysMutex.Lock()
	defer o.GatewaysMutex.Unlock()
	if o.Gateways == nil {
		o.Gateways = make(map[string]qtiny.Gateway)
	}
	var current = o.Gateways[gatekey]
	if current == nil {
		o.Gateways[gatekey] = gateway
	}
	return nil
}

func (o *MemDiscovery) GatewayUnpublish(gatekey string) error {
	o.GatewaysMutex.Lock()
	defer o.GatewaysMutex.Unlock()
	if o.Gateways == nil {
		return nil
	}
	var current = o.Gateways[gatekey]
	if current == nil {
		return nil
	}
	_ = current.EventChannelClose(gatekey)
	delete(o.Gateways, gatekey)
	return nil
}

func (o *MemDiscovery) gatewayEventLoop(gateway qtiny.Gateway, ch <-chan *qtiny.GatewayEventBox) {

	defer func() {
		o.PortalRemove(gateway.GetNodeId())
	}()

	for box := range ch {
		func() {
			defer func() {
				var pan = recover()
				if pan != nil && o.Logger != nil {
					o.Logger.Printf("gateway publish loop error %v \n %v", util.AsError(pan).Error(), qerr.StackString(1, 1024, ""))
				}
			}()

			if box.Event == qtiny.GatewayEventDisconnected {
				o.PortalRemove(gateway.GetNodeId())
			}

			if box.Event == qtiny.GatewayEventConnected {
				var portal = o.PortalCreate(gateway.GetNodeId())
				portal.SetMeta(gateway.GetMeta())
			}
		}()
	}
}

/* ======================== portals ==================================== */

func (o *MemDiscovery) PortalGet(address string) qtiny.PortalKind {
	if o.Portals == nil {
		return nil
	}
	o.PortalsMutex.RLock()
	defer o.PortalsMutex.RUnlock()
	var portal = o.Portals[address]
	if portal == nil {
		return nil
	}
	return portal
}

func (o *MemDiscovery) PortalCreate(addresss string) *qtiny.Portal {
	var kind = o.PortalGet(addresss)
	if kind != nil {
		return kind.(*qtiny.Portal)
	}
	var portal = &qtiny.Portal{Address: addresss}
	o.PortalSet(portal)
	return portal
}

func (o *MemDiscovery) PortalSet(portal *qtiny.Portal) {

	if portal == nil {
		panic("invalid argument, portal is nil")
	}

	if len(portal.Address) == 0 {
		panic("invalid argument, portal address is empty")
	}

	o.PortalsMutex.Lock()
	defer o.PortalsMutex.Unlock()
	if o.Portals == nil {
		o.Portals = make(map[string]*qtiny.Portal)
	}
	portal.Discoverer = o
	o.Portals[portal.Address] = portal
}

func (o *MemDiscovery) PortalRemove(address string) {
	if o.Portals == nil {
		return
	}
	o.PortalsMutex.Lock()
	defer o.PortalsMutex.Unlock()
	delete(o.Portals, address)
}

func (o *MemDiscovery) GatewayGet(gatekey string) qtiny.Gateway {
	if o.Gateways == nil {
		return nil
	}

	o.GatewaysMutex.RLock()
	var gateway = o.Gateways[gatekey]
	o.GatewaysMutex.RUnlock()
	return gateway
}

/* ============================================================================================= */

func (o *MemDiscovery) GetNodeId() string {
	return o.id
}

func (o *MemDiscovery) SetNodeId(id string) {
	o.id = id
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
