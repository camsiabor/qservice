package qtiny

import (
	"github.com/camsiabor/qcom/util"
	"github.com/twinj/uuid"
	"log"
	"os"
	"sync"
)

var tina = &Tina{}

func GetTina() *Tina {
	return tina
}

type Tina struct {
	nodeId string

	tinaMutex sync.RWMutex

	tinyMutex sync.RWMutex
	tinys     map[string]*Tiny

	gatewaysMutex sync.RWMutex
	gateways      map[string]Gateway
	gatewaydef    string

	discovery   Discovery
	microroller *Microroller

	config map[string]interface{}

	logger *log.Logger
}

func (o *Tina) Start(config map[string]interface{}) error {

	o.tinaMutex.Lock()
	defer o.tinaMutex.Unlock()

	if o.tinys == nil {
		o.tinys = make(map[string]*Tiny)
	}

	o.config = config
	if o.config == nil {
		o.config = make(map[string]interface{})
	}

	o.nodeId = util.GetStr(o.config, "", "id")
	if len(o.nodeId) == 0 {
		o.nodeId = uuid.NewV4().String()
	}

	var loggerConfig = util.GetMap(o.config, true, "logger")
	var err = o.initLogger(loggerConfig)
	if err != nil {
		return err
	}

	err = o.initMicroroller(o.config)
	return err
}

func (o *Tina) Stop() error {

	o.tinaMutex.Lock()
	defer o.tinaMutex.Unlock()

	return nil
}

func (o *Tina) initLogger(map[string]interface{}) error {
	if o.logger != nil {
		return nil
	}
	// TODO
	o.logger = log.New(os.Stdout, "[tina] ", log.Lshortfile|log.LstdFlags)
	return nil
}

func (o *Tina) initMicroroller(config map[string]interface{}) error {

	o.logger.Printf("tina %v initiating", o.nodeId)

	if o.gateways == nil {
		panic("gateway is not set")
	}

	if o.discovery == nil {
		panic("discovery is not set")
	}

	// discovery
	o.discovery.SetNodeId(o.nodeId)

	for gatekey, gateway := range o.gateways {
		gateway.SetId(gatekey)
		gateway.SetNodeId(o.nodeId)
		if gateway.GetLogger() == nil {
			gateway.SetLogger(o.logger)
		}
		if err := o.discovery.GatewayPublish(gatekey, gateway); err != nil {
			return err
		}
	}

	if o.discovery.GetLogger() == nil {
		o.discovery.SetLogger(o.logger)
	}

	var discoveryConfig = util.GetMap(o.config, true, "discovery")
	if err := o.discovery.Start(discoveryConfig); err != nil {
		return err
	}

	for gatekey, gateway := range o.gateways {

		var gatewayConfig = util.GetMap(o.config, true, "gateways", gatekey)
		if err := gateway.Start(gatewayConfig); err != nil {
			return err
		}
	}

	// microroller
	if o.microroller == nil {
		o.microroller = &Microroller{}
	}
	if o.microroller.GetLogger() == nil {
		o.microroller.SetLogger(o.logger)
	}
	o.microroller.SetGateways(o.gateways, o.gatewaydef)
	o.microroller.SetDiscovery(o.discovery)
	var microrollerConfig = util.GetMap(o.config, true, "microroller")
	return o.microroller.Start(microrollerConfig)
}

func (o *Tina) Deploy(id string, guide TinyGuideKind, config map[string]interface{}, flag TinyFlag, options TinyOptions) *Future {

	var future = &Future{}
	future.Name = "tina.deploy"
	future.routine = func(event FutureEvent, future *Future) FutureCallbackReturn {

		o.tinyMutex.RLock()
		var current = o.tinys[id]
		o.tinyMutex.RUnlock()

		if current != nil {
			future.Fail(0, "already deployed a tiny with id : "+id)
			return 0
		}

		if len(id) == 0 {
			id = uuid.NewV4().String()
		}

		var tiny = &Tiny{}
		tiny.id = id
		tiny.flag = flag
		tiny.config = config
		tiny.options = options
		tiny.guide = guide
		tiny.tina = o

		o.tinyMutex.Lock()
		o.tinys[tiny.id] = tiny
		o.tinyMutex.Unlock()

		if flag&TinyFlagDeploySync > 0 {
			tiny.Start(future)
		} else {
			go tiny.Start(future)
		}
		return 0
	}

	return future
}

func (o *Tina) Undeploy(tinyId string) *Future {

	var future = &Future{}
	future.Name = "tina.undeploy"

	o.tinyMutex.RLock()
	var tiny = o.tinys[tinyId]
	o.tinyMutex.RUnlock()

	if tiny == nil {
		future.isSucceed = true
		return future
	}

	future.routine = func(event FutureEvent, future *Future) FutureCallbackReturn {

		o.tinyMutex.Lock()
		delete(o.tinys, tinyId)
		o.tinyMutex.Unlock()

		tiny.Stop(future)

		return 0
	}

	return future
}

func (o *Tina) SetGateways(gateways map[string]Gateway, gatewaydef string) *Tina {

	if len(gatewaydef) > 0 && gateways[gatewaydef] == nil {
		panic("default gateway not found in arguments " + gatewaydef)
	}

	for gatekey, gateway := range gateways {
		gateway.SetId(gatekey)
		gateway.SetNodeId(o.nodeId)
		if len(gatewaydef) == 0 {
			gatewaydef = gatekey
		}
	}

	o.gatewaysMutex.Lock()
	defer o.gatewaysMutex.Unlock()

	o.gateways = gateways
	o.gatewaydef = gatewaydef
	return o
}

func (o *Tina) GetGateways() map[string]Gateway {
	return o.gateways
}

func (o *Tina) GetDiscovery() Discovery {
	return o.discovery
}

func (o *Tina) SetDiscovery(discovery Discovery) {
	o.discovery = discovery
}

func (o *Tina) SetMicroroller(microroller *Microroller) *Tina {
	o.microroller = microroller
	return o
}

func (o *Tina) GetMicroroller() *Microroller {
	return o.microroller
}

func (o *Tina) GetLogger() *log.Logger {
	return o.logger
}

func (o *Tina) SetLogger(logger *log.Logger) *Tina {
	o.logger = logger
	return o
}

func (o *Tina) GetNodeId() string {
	return o.nodeId
}
